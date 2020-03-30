package rsmq

// The changeMessageVisibility LUA Script

// Parameters:

// KEYS[1]: the zset key
// KEYS[2]: the message id

// * Find the message id
// * Set the new timer

// Returns:

// 0 or 1
const changeMessageVisibilityScript = `
local msg = redis.call("ZSCORE", KEYS[1], KEYS[2])

if not msg then
  return 0
end

redis.call("ZADD", KEYS[1], KEYS[3], KEYS[2])

return 1
`

// The popMessage LUA Script

// Parameters:

// KEYS[1]: the zset key
// KEYS[2]: the current time in ms

// * Find a message id
// * Get the message
// * Increase the rc (receive count)
// * Use hset to set the fr (first receive) time
// * Return the message and the counters

// Returns:

// {id, message, rc, fr}
const popMessageScript = `
local msg = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2], "LIMIT", "0", "1")

if #msg == 0 then
  return {}
end

redis.call("HINCRBY", KEYS[1] .. ":Q", "totalrecv", 1)

local mbody = redis.call("HGET", KEYS[1] .. ":Q", msg[1])
local rc = redis.call("HINCRBY", KEYS[1] .. ":Q", msg[1] .. ":rc", 1)
local o = {msg[1], mbody, rc}

if rc==1 then
  table.insert(o, KEYS[2])
else
  local fr = redis.call("HGET", KEYS[1] .. ":Q", msg[1] .. ":fr")
  table.insert(o, fr)
end

redis.call("ZREM", KEYS[1], msg[1])
redis.call("HDEL", KEYS[1] .. ":Q", msg[1], msg[1] .. ":rc", msg[1] .. ":fr")

return o
`

// The receiveMessage LUA Script

// Parameters:

// KEYS[1]: the zset key
// KEYS[2]: the current time in ms
// KEYS[3]: the new calculated time when the vt runs out

// * Find a message id
// * Get the message
// * Increase the rc (receive count)
// * Use hset to set the fr (first receive) time
// * Return the message and the counters

// Returns:

// {id, message, rc, fr}
const receiveMessageScript = `
local msg = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2], "LIMIT", "0", "1")

if #msg == 0 then
  return {}
end

redis.call("ZADD", KEYS[1], KEYS[3], msg[1])
redis.call("HINCRBY", KEYS[1] .. ":Q", "totalrecv", 1)

local mbody = redis.call("HGET", KEYS[1] .. ":Q", msg[1])
local rc = redis.call("HINCRBY", KEYS[1] .. ":Q", msg[1] .. ":rc", 1)
local o = {msg[1], mbody, rc}

if rc==1 then
  redis.call("HSET", KEYS[1] .. ":Q", msg[1] .. ":fr", KEYS[2])
  table.insert(o, KEYS[2])
else
  local fr = redis.call("HGET", KEYS[1] .. ":Q", msg[1] .. ":fr")
  table.insert(o, fr)
end

return o
`
