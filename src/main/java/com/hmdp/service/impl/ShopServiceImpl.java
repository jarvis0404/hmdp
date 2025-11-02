package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("shop id is null");
        }

        // 1. update mysql
        this.updateById(shop);

        // 2. delete redis cache
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1. if point is not null
        if (x == null || y == null) {
            // 根据类型分页查询
            Page<Shop> page = this.query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        // 2. page parameter
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3. query redis: shop id, distance
        // geosearch bylonlat x y byradius 10 withdistance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );

        if (results == null) {
            return Result.ok(Collections.emptyList());
        }

        // 4. get nearest shop ids
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> shopIds = new ArrayList<>(list.size());
        Map<String, Distance> map = new HashMap<>(list.size());
        list.stream().skip(from).forEach(geoResult -> {
            String shopIdStr = geoResult.getContent().getName();
            shopIds.add(Long.valueOf(shopIdStr));
            Distance distance = geoResult.getDistance();
            map.put(shopIdStr, distance);
        }); // 使用 stream 的 skip 逻辑分页

        // 5. query shop by shop id from mysql
        String shopIdsStr = StrUtil.join(", ", shopIds);
        List<Shop> shops = this.query()
                .in("id", shopIds)
                .last("order by field(id, " + shopIdsStr + " )")
                .list();
        // set shop distance
        for (Shop shop : shops) {
            shop.setDistance(map.get(shop.getId().toString()).getValue());
        }

        return Result.ok(shops);
    }

    @Override
    public Result queryById(Long id) {

        // 考虑缓存穿透
        // Shop shop = queryWithPenetration(id);
        Shop shop = cacheClient.queryWithPenetration(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 用互斥锁解决缓存击穿
        // Shop shop = queryWithBreakdownMutex(id);

        // 用逻辑过期解决缓存击穿（热点key）
        // Shop shop = queryWithBreakdownLogicExpire(id);
        // Shop shop = cacheClient.queryBreakdownWithLogicExpire(CACHE_SHOP_KEY, id, Shop.class, LOCK_SHOP_KEY, this::getById, LOCK_SHOP_TTL, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("shop is null");
        }

        return Result.ok(shop);
    }

    public static final ExecutorService CACHE_REBUILD_EXECUTOR =  Executors.newFixedThreadPool(10);

    private Shop queryWithBreakdownLogicExpire(Long id) {
        // 1. query from redis
        String redisDataJSON = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // 2. 判断是否命中
        // 热点key由后台管理系统添加，一般不会不命中
        if (StrUtil.isBlank(redisDataJSON)) {
            // 3. 未命中，返回空
            return null;
        }

        // 4. 命中，解析json，获得shop对象
        RedisData redisData = JSONUtil.toBean(redisDataJSON, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);

        // 5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期，返回
            return shop;
        }

        // 5.2 已过期，重建缓存

        // 6. 缓存重建
        // 6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean getLock = tryLock(lockKey);
        // 6.2 判断互斥锁是否获取成功
        if (!getLock) {
            // 6.3 不成功，返回旧数据
            return shop;
        }

        // 6.4 成功，开启独立线程，实现缓存重建
        CACHE_REBUILD_EXECUTOR.execute(() -> {
            try {
                this.saveShop2Redis(id, 30L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                this.unlock(lockKey);
            }
        });

        // 返回旧数据
        return shop;
    }

    private Shop queryWithBreakdownMutex(Long id) {
        // 1. query from redis
        String shopJSON = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // 1.1 redis exist, return
        if (StrUtil.isNotBlank(shopJSON)) {
            return JSONUtil.toBean(shopJSON, Shop.class);
        }

        // redis cache exist, but it's blank, to avoid cache penetration
        if (shopJSON != null) {
            return null;
        }

        // 1.2 redis doesn't exist, get from mysql
        Shop shop = null;
        String lockKey = LOCK_SHOP_KEY + id;
        try {
            boolean lockGet = tryLock(lockKey);
            if (!lockGet) {
                Thread.sleep(50); // 等待一段时间后重新查询
                return queryWithBreakdownMutex(id);
            }

            // 模拟查询数据库的耗时
            // Thread.sleep(200);
            shop = this.getById(id);

            // 2.1 mysql doesn't exist
            if (shop == null) {
                // to avoid cache penetration
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }

            // 2.2 mysql exist shop info, cache it to redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(lockKey);
        }

        return shop;
    }

    private Shop queryWithPenetration(Long id) {
        // 1. query from redis
        String shopJSON = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // 1.1 redis exist, return
        if (StrUtil.isNotBlank(shopJSON)) {
            return JSONUtil.toBean(shopJSON, Shop.class);
        }

        // redis cache exist, but it's blank, to avoid cache penetration
        if (shopJSON != null) {
            return null;
        }

        // 1.2 redis doesn't exist, get from mysql
        Shop shop = this.getById(id);

        // 2.1 mysql doesn't exist
        if (shop == null) {
            // to avoid cache penetration
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 2.2 mysql exist shop info, cache it to redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return shop;
    }

    private boolean tryLock(String key) {
        Boolean res = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(res);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id, Long expireTime) throws InterruptedException {
        Shop shop = this.getById(id);

        // 模拟网络延时
        // Thread.sleep(200);

        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));

        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

}
