package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1. check whether phone number is valid
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2. if invalid return fail message
            return Result.fail("phone number is invalid");
        }

        // 3. if valid
        // 3.1 generate code
        String code = RandomUtil.randomNumbers(6);

        // 3.2 save code to session

        // session.setAttribute("code", code);

        // 3.2 save code to redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);

        // 3.3 send code
        log.debug("login code sent: {}", code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1. check whether phone number is valid
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("phone number is invalid");
        }

        // 2. check code with commited phone & code in redis
        String codeRedis = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if (!Objects.equals(codeRedis, loginForm.getCode())) {
            return Result.fail("code is wrong");
        }

        // 3. login or register
        // 3.1 query user using phone number
        User user = query().eq("phone", phone).one();

        // 3.2 current user doesn't exist, create a new one
        if (user == null) {
            user = createUserWithPhone(phone);
            this.save(user);
        }

        // 4. save user to redis
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        String token = UUID.randomUUID().toString(true);
        String tokenKey = LOGIN_USER_KEY + token;
        Map<String, Object> objectMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString())
        );
        stringRedisTemplate.opsForHash().putAll(tokenKey, objectMap);
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);

        // return token to client
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 1. get current user
        Long userId = UserHolder.getUser().getId();
        // 2. get date
        LocalDate date = LocalDate.now();
        // 3. set redis key
        String suffix = date.format(DateTimeFormatter.ofPattern(":yyyy-MM"));
        String key = USER_SIGN_KEY + userId + suffix;
        // 4. write into redis
        int dayOfMonth = date.getDayOfMonth();
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 1. get current user
        Long userId = UserHolder.getUser().getId();
        // 2. get date
        LocalDate date = LocalDate.now();
        // 3. set redis key
        String suffix = date.format(DateTimeFormatter.ofPattern(":yyyy-MM"));
        String key = USER_SIGN_KEY + userId + suffix;

        // 4. get
        int dayOfMonth = date.getDayOfMonth();
        // bitfield key get type index
        List<Long> results = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if (results == null || results.isEmpty()) {
            return Result.ok(0);
        }

        Long res = results.get(0);
        if (res == null || res == 0) {
            return Result.ok(0);
        }
        int cnt = 0;

        while (true) {
            if ((res & 1) == 0) {
                break;
            } else {
                cnt++;
            }
            res = res >>> 1;
        }

        return Result.ok(cnt);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        return user;
    }

}
