-- Thredis Utility LUA Module.
$vars config
-- KEY_SEPARATOR

-- Atomic Redis Copy from ``source`` key to ``dest`` key.
local function copy(source, dest)
    return redis.call('RESTORE', dest, redis.call('DUMP', source))
end

local function gen_key(namespace)
    return table.concat(namespace, KEY_SEPARATOR)
end