package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    public static final long BEGIN_TIMESTAMP = 1735689600L;
    public static final int COUNT_BITS = 32;

    StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     *
     * @param keyPrefix service name
     * @return an unique id
     */
    public long nextId(String keyPrefix) {
        // 1. 生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long timeStamp = now.toEpochSecond(ZoneOffset.UTC) - BEGIN_TIMESTAMP;

        // 2. 生成序列号
        // 2.1 获取日期
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 2.2 redis对某个key的value做自增，key与date有关
        // cnt：某一天的计数器
        long cnt = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        // 3. 拼接并范围
        return timeStamp << COUNT_BITS | cnt;
    }

    public static void main(String[] args) {
        LocalDateTime time = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
        long second = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println("second = " + second);
    }

}
