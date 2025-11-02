package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result listQuery() {
        // 1. query from redis
        String shopTypesJSON = stringRedisTemplate.opsForValue().get("cache:shop-type");

        // 2. redis exist, return
        if (StrUtil.isNotBlank(shopTypesJSON)) {
            List<ShopType> list = JSONUtil.parseArray(shopTypesJSON).toList(ShopType.class);
            return Result.ok(list);
        }

        // 3. redis doesn't exist

        // 3.1 query from mysql
        List<ShopType> typeList = this.query().orderByAsc("sort").list();

        // 3.2 mysql doesn't exist
        if (typeList == null || typeList.isEmpty()) {
            return Result.fail("no shop type found");
        }

        // 3.3 mysql exist, save it to redis
        stringRedisTemplate.opsForValue().set("cache:shop-type", JSONUtil.toJsonStr(typeList));

        return Result.ok(typeList);
    }
}
