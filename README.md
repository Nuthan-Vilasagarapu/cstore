# cStore (Cache Store)

A Simple Redis Server Implementation

## CMD implemented

Follow https://redis.io/docs/latest/commands/redis-8-8-commands/

- [x] PING
- [x] SET
- [x] GET <!--  not handeled with expiry time -->
- [ ] SETEX <!--  doubt how to implement time seconds -->
- [x] SETNX
- [x] DEL 
- [x] ECHO
- [x] EXISTS
- [x] KEYS <!-- pattern matching not done -->
- [x] APPEND 
- [x] STRLEN
- [x] MSET
- [x] MGET
- [x] MSETNX
- [ ] MSETEX <!--  doubt how to implement for time seconds -->
- [x] FLUSHALL
- [x] INCR
- [x] INCRBY
- [x] DECR
- [x] DECRBY
- [x] EXPIRE <!-- no {nx, xx, gt, lt} and some doubt init still-->
- [x] TTL <!-- only some done not full -->
- [x] PERSIST
- [x] HSET
- [x] HGET
- [ ] HGETALL
- [ ] HMSET
- [ ] HSETNX
- [ ] HSETEX
- [ ] HDEL
- [ ] HKEYS
- [ ] HLEN
- [ ] HVALS

## Future Goals
- Implement DB
- Save as cache dump to a file 