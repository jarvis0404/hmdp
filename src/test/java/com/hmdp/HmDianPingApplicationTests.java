package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    ShopServiceImpl shopServiceImpl;

    @Resource
    RedisIdWorker redisIdWorker;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    public static final ExecutorService es = Executors.newFixedThreadPool(300);

    @Test
    void testRedisIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long uniqueId = redisIdWorker.nextId("order");
                System.out.println("id = " + uniqueId);
            }
            latch.countDown();
        };

        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.execute(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("run time: " + (end - begin));
    }

    @Test
    void testSaveShop2Redis() throws InterruptedException {
        // 模拟后台手动导入热点数据
        shopServiceImpl.saveShop2Redis(1L, 10L);
    }

    @Test
    void loadShopGeo2Redis() {
        // 1. query shops
        List<Shop> list = shopServiceImpl.list();

        // 2. group shops
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));

        // 3. load to redis geo
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            List<Shop> shops = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());

            for (Shop shop : shops) {
                // stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())));
            }

            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }

    @Test
    void testHyperLogLogRedis() {
        String[] users = new String[1000];
        int index = 0;
        for (int i = 0; i < 1000000; i++) {
            index = i % 1000;
            users[index] = "user" + i;
            if (index == 999) {
                stringRedisTemplate.opsForHyperLogLog().add("hll1", users);
            }
        }

        Long cnt = stringRedisTemplate.opsForHyperLogLog().size("hll1");

        System.out.println("cnt = " + cnt);
    }

}
