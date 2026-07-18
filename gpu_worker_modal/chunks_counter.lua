local document_id = KEYS[1]
local limit = tonumber(ARGV[1])
local counter = redis.call('INCR', document_id)
if counter == 1 then
    redis.call('EXPIRE', document_id, 21600)
end
if counter >= limit then
    return 1
end
return 0