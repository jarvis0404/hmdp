package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private String lockName; // 通常为锁的业务名称
    private StringRedisTemplate stringRedisTemplate;
    public static final DefaultRedisScript<Long> UNLOCK_SCRIPT; // 提前加载lua脚本
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(String lockName, StringRedisTemplate stringRedisTemplate) {
        this.lockName = lockName;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public static final String LOCK_PREFIX = "lock:";
    public static final String UUID_PREFIX = UUID.randomUUID().toString(true);

    @Override
    public boolean tryLock(long timeoutSeconds) {
        String lockKey = LOCK_PREFIX + lockName;
        long threadID = Thread.currentThread().getId();
        String lockValue = UUID_PREFIX + "-" + threadID;
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(lockKey, lockValue, timeoutSeconds, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    /*
    @Override
    public void unlock() {
        String lockKey = LOCK_PREFIX + lockName;
        long threadId = Thread.currentThread().getId();
        String lockValue = UUID_PREFIX + "-" + threadId;

        String value = stringRedisTemplate.opsForValue().get(lockKey);
        if (lockValue.equals(value)) {
            stringRedisTemplate.delete(lockKey);
        }
    }
    */

    /**
     * unlock with lua script
     */
    @Override
    public void unlock() {
        String key = LOCK_PREFIX + lockName;
        long threadID = Thread.currentThread().getId();
        String arg = UUID_PREFIX + "-" + threadID;

        // execute lua script
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(key), arg);
    }
}
