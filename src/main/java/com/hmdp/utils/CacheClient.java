package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long expire, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), expire, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long expire, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(expire)));
        redisData.setData(value);
        // write to redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <ID, R> R queryWithPenetration(String keyPrefix, ID id,
                                          Class<R> resultClass,
                                          Function<ID, R> dbFallBack,
                                          Long expire, TimeUnit unit
    ) {
        // 1. query from redis
        String redisKey = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(redisKey);

        // 1.1 redis exist, return
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, resultClass);
        }

        // json can be found and it is blank
        if (json != null) {
            return null;
        }

        // 1.2 redis doesn't exist, get from mysql
        R r = dbFallBack.apply(id);

        // 2.1 mysql doesn't exist
        if (r == null) {
            // to avoid cache penetration
            stringRedisTemplate.opsForValue().set(redisKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 2.2 mysql exist shop info, cache it to redis
        this.set(redisKey, JSONUtil.toJsonStr(r), expire, unit);

        return r;
    }

    public static final ExecutorService CACHE_REBUILD_EXECUTOR =  Executors.newFixedThreadPool(10);

    public <ID, R> R queryBreakdownWithLogicExpire(String keyPrefix, ID id,
                                                   Class<R> returnClass,
                                                   String lockKeyPrefix,
                                                   Function<ID, R> dbFallBack,
                                                   Long expire, TimeUnit unit
                                                   ) {
        // 1. query from redis
        String redisKey = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(redisKey);

        // 2. 判断是否命中
        // 热点key由后台管理系统添加，一般不会不命中
        if (StrUtil.isBlank(json)) {
            // 3. 未命中，返回空
            return null;
        }

        // 4. 命中，解析json，获得shop对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), returnClass);

        // 5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期，返回
            return r;
        }

        // 5.2 已过期，重建缓存

        // 6. 缓存重建
        // 6.1 获取互斥锁
        String lockKey = lockKeyPrefix + id;
        boolean getLock = tryLock(lockKey);
        // 6.2 判断互斥锁是否获取成功
        if (!getLock) {
            // 6.3 不成功，返回旧数据
            return r;
        }

        // 6.4 成功，开启独立线程，实现缓存重建
        CACHE_REBUILD_EXECUTOR.execute(() -> {
            try {
                R r1 = dbFallBack.apply(id);
                this.setWithLogicalExpire(redisKey, r1, expire, unit);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                this.unlock(lockKey);
            }
        });

        // 返回旧数据
        return r;
    }


    private boolean tryLock(String key) {
        Boolean res = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(res);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }


}
