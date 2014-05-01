-- Thredis Utility LUA Module.


-- Atomic Redis Copy from ``source`` key to ``dest`` key.
local function copy(source, dest)
    return redis.call('RESTORE', dest, redis.call('DUMP', source))
end