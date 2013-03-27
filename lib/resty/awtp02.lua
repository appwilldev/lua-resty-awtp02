local setmetatable  = setmetatable
local type          = type
local math_floor    = math.floor
local string_char   = string.char
local string_byte   = string.byte
local tcp           = ngx.socket.tcp

local JSON = require("cjson")

local function parseNetInt(bytes)
    local a, b, c, d = string_byte(bytes, 1, 4)
    return a * 256 ^ 3 + b * 256 ^ 2 + c * 256 + d
end

local function toNetInt(n)
    -- NOTE: for little endian machine only!!!
    local d = n % 256
    n = math_floor(n / 256)
    local c = n % 256
    n = math_floor(n / 256)
    local b = n % 256
    n = math_floor(n / 256)
    local a = n
    return string_char(a) .. string_char(b) .. string_char(c) .. string_char(d)
end

local function write_jsonresponse(sock, s)
    if type(s)=='table' then
        s = JSON.encode(s)
    end
    local l = toNetInt(#s)
    local ok, err = sock:send(l .. s)
    if not ok then
        return nil, err
    end
    return ok
end

local function read_jsonresponse(sock)
    local r, err = sock:receive(4)
    if not r then
        return nil, err
    end

    local len = parseNetInt(r)
    local data, err = sock:receive(len)
    if not data then
        return nil, err
    end
    return JSON.decode(data)
end

local AWTP02 = "AWTP02"

module(...)

local mt = { __index = _M }

function new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end

function set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end

function connect(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local ok, err = sock:connect(...)
    if not ok then
        return nil, err
    end

    local reused_times, err = self:get_reused_times()
    if reused_times==nil then
        self:close()
        o.sock = nil
        return nil, err
    elseif reused_times<1 then
        local ok, err = self:handshake()
        if not ok then
            self:close()
            o.sock = nil
            return nil, err
        end
    end

    return ok
end

function set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end

function get_reused_times(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end

function close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end

function handshake(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local ok, err = nil, nil
    ok, err = sock:send(AWTP02)
    if not ok then
        return nil, err
    end

    ok, err = sock:receive(6)
    if not ok then
        return nil, err
    end

    if ok ~= AWTP02 then
        return nil, 'Handshake failed.'
    end

    return true
end

function do_cmd(self, data)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local ok, err = write_jsonresponse(sock, data)
    if not ok then
        return nil, err
    end

    return read_jsonresponse(sock)
end

local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}

setmetatable(_M, class_mt)

