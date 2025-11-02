package com.hmdp.utils;

public interface ILock {

    /**
     *
     * @param timeoutSeconds Time To Live
     * @return whether trying to get lock success
     */
    boolean tryLock(long timeoutSeconds);

    /**
     * release lock
     */
    void unlock();

}
