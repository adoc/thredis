$vars config
-- NONCES_KEY
-- LOCK_PRE
-- LOCK_TS_PRE


local function nonce_get()
    return redis.call("LPOP", NONCES_KEY)
end


-- Attempt to acquire a lock on resource_key
local function lock_acquire(resource, timeout)
    local timeout = tostring(timeout)
    local lock_key = LOCK_PRE ..":".. resource
    local lock_key_ts = LOCK_TS_PRE ..":".. resource
    nonce = nonce_get()
    -- Set lock to nonce(id) with deadlock timeout.
    if nonce then
        if redis.call("SET", lock_key, nonce, "NX", "PX", timeout) then
            return nonce
        else
            -- We got a nonce, but could not get the lock, return FALSE.
            return false
        end
    else
        -- No nonce available, return NIL. TODO: Expand with errors.
        return nil
    end
end


local function lock_release()
    -- Attempt to release the lock.
    local resource_key = KEYS[1]
    local key = "lock:" .. resource_key
    local nonce = ARGV[1]
    if redis.call("GET", key) == nonce then
        return redis.call("DEL", key)
    else
        return nil
    end
end