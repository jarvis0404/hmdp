package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    /**
     *
     * @param id blog id
     * @return queried blog
     */
    @Override
    public Result queryById(Long id) {
        // 1. query blog
        Blog blog = this.getById(id);
        if (blog == null) {
            return Result.fail("blog not exist");
        }

        // 2. query blog user
        this.queryBlogUser(blog);
        // 3. if current liked this blog
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    /**
     * if given blog is liked by current user
     * if yes, set isLiked filed for given blog entity
     * @param blog given blog
     */
    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // user not login
            return;
        }
        // 1. get current user id
        Long userId = user.getId();
        // 2. if current user has liked
        String key = BLOG_LIKED_KEY + blog.getId();
        // Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        // blog.setIsLike(BooleanUtil.isTrue(isMember));
        blog.setIsLike(score != null);
    }

    /**
     * get hot blog's basic info
     * @param current page
     * @return blog records
     */
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = this.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    /**
     *
     * @param id blog id
     * @return result
     */
    @Override
    public Result likeBlog(Long id) {
        // 1. get current user id
        Long userId = UserHolder.getUser().getId();

        // 2. if current user has liked
        String key = BLOG_LIKED_KEY + id;
        // Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        // if (BooleanUtil.isFalse(isMember)) {
        if (score == null) {
            // 3. not liked
            boolean success = this.update().setSql("liked = liked + 1").eq("id", id).update();
            if (success) {
                // stringRedisTemplate.opsForSet().add(key, userId.toString());
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 4. liked
            boolean success = this.update().setSql("liked = liked - 1").eq("id", id).update();
            if (success) {
                // stringRedisTemplate.opsForSet().remove(key, userId.toString());
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }

        return Result.ok();
    }

    /**
     * get top 5 liked users' icon and nickname
     * @param id blog id
     * @return user DTOs
     */
    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        // 1. get top 5
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0L, 4L);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // 2.
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String strIds = StrUtil.join(",", ids);
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id, "+ strIds +")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean saved = save(blog);
        if (!saved) {
            return Result.fail("blog save unsuccess");
        }

        // 查询笔记作者的粉丝
        // select * from tb_follow where follow_user_id = ?
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        for (Follow follow : follows) {
            Long userId = follow.getUserId();
            String key = "feed:" + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }

        return Result.ok("save blog successful");
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1. get user
        Long userId = UserHolder.getUser().getId();
        // 2. get feed inbox
        String key = FEED_KEY + userId;
        // ZREVRANGEBYSCORE key max min LIMIT offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate
                .opsForZSet().reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }

        // 3. parse data, blog id, minTime, offset
        long minTime = 0;
        int cnt = 1;
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            // blog id
            blogIds.add(Long.parseLong(tuple.getValue()));
            // score(timestamp)
            long timestamp = tuple.getScore().longValue();
            // offset for next page
            if (timestamp == minTime) {
                cnt++;
            } else {
                minTime = timestamp;
            }
        }

        String strIds = StrUtil.join(",", blogIds);
        List<Blog> blogs = this.query()
                .in("id", blogIds).last("ORDER BY FIELD(id, " + strIds + ")").list();

        for (Blog blog : blogs) {
            queryBlogUser(blog);
            isBlogLiked(blog);
        }

        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(cnt);

        return Result.ok(scrollResult);
    }

    /**
     * get given blog's user's icon and nickname
     * @param blog blog entity
     */
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
