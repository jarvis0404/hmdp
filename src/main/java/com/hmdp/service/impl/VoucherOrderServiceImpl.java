package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    public static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    public void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        public static final String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    // 获取消息队列中待更新到数据库中的order
                    // XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> lst = stringRedisTemplate.opsForStream().read(
                            Consumer.from("group1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    // 2. 判断消息获取是否成功
                    if (lst == null || lst.isEmpty()) {
                        // 2.1 获取失败，没有消息，继续下一次循环
                        continue;
                    }
                    // 3. 有消息，下单，在数据库中创建订单
                    // 解析消息
                    MapRecord<String, Object, Object> record = lst.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    // 4. ACK确认读取出的消息被消费掉了
                    // SACK stream.orders group1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "group1", record.getId());
                } catch (Exception e) {
                    log.error("VoucherOrderHandler error", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 获取消息队列中待更新到数据库中的order
                    // XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> lst = stringRedisTemplate.opsForStream().read(
                            Consumer.from("group1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 2. 判断消息获取是否成功
                    if (lst == null || lst.isEmpty()) {
                        // 2.1 获取失败，pending list没有消息，继续下一次循环
                        break;
                    }

                    // 3. 有消息，下单，在数据库中创建订单
                    // 解析消息
                    MapRecord<String, Object, Object> record = lst.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    // 4. ACK确认读取出的消息被消费掉了
                    // SACK stream.orders group1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "group1", record.getId());
                } catch (Exception e) {
                    log.error("VoucherOrderHandler error", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    /*
    // jdk的阻塞队列可能内存溢出，数据安全问题
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 获取消息队列中待更新到数据库中的order
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 更新数据库
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("VoucherOrderHandler error", e);
                }
            }
        }
    }
    */

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 获取分布式锁，但理论上通过lua脚本在redis中已经可以保证不会发生超卖问题
        Long userId = voucherOrder.getUserId(); // 子线程无法获取thread local
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean lockGet = lock.tryLock();
        if (!lockGet) {
            log.error("VoucherOrderHandler get lock error");
            return;
        }

        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1. execute lua script
        // 判断是否有秒杀资格
        long orderId = redisIdWorker.nextId("order");
        Long res = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        ); // 执行完后消息已添加到了mq

        // 2. lua result
        int resStatus = res.intValue();
        // 2.1 res is not 0
        if (resStatus != 0) {
            return Result.fail(resStatus == 1 ? "lack of stock" : "already bought before");
        }

        // 3. 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 4. return order id to front end
        return Result.ok(orderId);
    }

    /*
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1. execute lua script
        Long userId = UserHolder.getUser().getId();
        // 判断是否有秒杀资格
        Long res = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        // 2. lua result
        int resStatus = res.intValue();
        // 2.1 res is not 0
        if (resStatus != 0) {
            return Result.fail(resStatus == 1 ? "lack of stock" : "already bought before");
        }

        // 2.2 res is 0
        // submit voucher order to block queue
        // 2.3 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3.1 代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.3.2 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.3.3 用户id
        voucherOrder.setUserId(userId);

        // 2.4 add order into blocking queue
        orderTasks.add(voucherOrder);

        // 3. 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 4. return order id to front end
        return Result.ok(orderId);
    }
    */

    /*
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1. 查询优惠券
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);

        // 2. 判断是否开始
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("还未开始");
        }

        // 3. 判断秒杀是否结束
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("seckill ended");
        }

        // 4. 判断还有库存
        if (seckillVoucher.getStock() < 1) {
            return Result.fail("sold out of stock");
        }

        Long userId = UserHolder.getUser().getId();

        // 通过代理对象实现mysql的事务
        IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
        return proxy.createVoucherOrder(voucherId, userId);
    }
    */

    // 在mysql中创建订单，供前端调用
    @Transactional
    public Result createVoucherOrder(Long voucherId, Long userId) {
        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean lockGet = lock.tryLock();
        if (!lockGet) {
            return Result.fail("手速太快啦，当前用户已购买！");
        }

        try {
            // 5. 判断用户是否已经购买过了当前秒杀优惠券（保证一人一单）
            // 存在超卖问题，用分布式锁解决
            int cnt = this.query().eq("user_id", userId)
                    .eq("voucher_id", voucherId)
                    .count();
            if (cnt > 0) {
                return Result.fail("无法重复购买当前优惠券");
            }

            // 6. 扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    // .eq("stock", seckillVoucher.getStock()) // 乐观锁，防止超卖
                    .gt("stock", 0) // 乐观锁，提高成功率
                    .update();

            if (!success) {
                return Result.fail("sold out of stock");
            }

            // 7. 创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1 代金券id
            voucherOrder.setVoucherId(voucherId);
            // 7.2 订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.3 用户id
            voucherOrder.setUserId(userId);
            // 7.4 存入mysql
            this.save(voucherOrder);

            return Result.ok(orderId);
        } finally {
            lock.unlock();
        }
    }

    // 在mysql中创建订单，供消息队列的消费者调用
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 判断用户是否已经购买过了当前秒杀优惠券（保证一人一单）
        // 存在超卖问题，用分布式锁解决
        int cnt = this.query().eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if (cnt > 0) {
            log.error("createVoucherOrder error, already bought before");
            return;
        }

        // 6. 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                // .eq("stock", seckillVoucher.getStock()) // 乐观锁，防止超卖
                .gt("stock", 0) // 乐观锁，提高成功率
                .update();

        if (!success) {
            log.error("createVoucherOrder error, stock sold out");
            return;
        }

        this.save(voucherOrder);
    }
}
