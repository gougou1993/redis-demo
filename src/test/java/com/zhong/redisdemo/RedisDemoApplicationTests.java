package com.zhong.redisdemo;

import com.zhong.redisdemo.entity.User;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.*;

@SpringBootTest
class RedisDemoApplicationTests {

    @Autowired
    private RedisTemplate redisTemplate;

    /**
     * 外部连接被拒绝，原因是默认只能连接本地的，修改下redis.conf（/etc/redis.conf）即可。
     * https://www.wandouip.com/t5i261984/
     * Caused by: io.lettuce.core.RedisConnectionException: Unable to connect to 127.0.0.1:6379
     */

    @Test
    void stringTest() {
        ValueOperations<String, User> valueOperations = redisTemplate.opsForValue();
        User user = new User(1, "zhong", "man");
        String k = "test:string:zhong";
        valueOperations.set(k, user);
        Object o = redisTemplate.opsForValue().get(k);
        ListOperations listOperations = redisTemplate.opsForList();
        System.out.println(o instanceof User);
    }

    @Test
    void listTest() {
        String users = "test:list:users";

        User user1 = new User(1, "zhong", "man");
        User user2 = new User(2, "zhong", "man");
        User user3 = new User(3, "zhong", "man");

        redisTemplate.opsForList().rightPush(users, user1);
        redisTemplate.opsForList().rightPush(users, user2);
        redisTemplate.opsForList().rightPush(users, user3);

        Object o = redisTemplate.opsForList().leftPop(users);
        System.out.println(o instanceof User);

    }

    @Test
    void setTest() {
        String ids = "test:set:users";
        SetOperations<String, User> setOperations = redisTemplate.opsForSet();

        User user1 = new User(1, "zhong", "man");
        User user2 = new User(2, "zhong", "man");
        User user3 = new User(3, "zhong", "man");
        setOperations.add(ids, user1, user2, user3);

    }

    @Test
    void hashTest() {
        String hash = "test:hash:users";
        HashOperations<String, String, User> hashOperations = redisTemplate.opsForHash();

        User user1 = new User(1, "zhong", "man");
        User user2 = new User(1, "zhong", "man");
        User user3 = new User(1, "zhong", "man");

        hashOperations.put(hash, "user1", user1);
        hashOperations.put(hash, "user2", user2);
        hashOperations.put(hash, "user3", user3);

        User user11 = hashOperations.get(hash, "user1");

        System.out.println(user11);


    }


    @Test
    void zSetTest() {
        String score = "test:zset:score";
        ZSetOperations zSetOperations = redisTemplate.opsForZSet();
        zSetOperations.add(score, "zs", 100);
        zSetOperations.add(score, "ls", 80);
        zSetOperations.add(score, "ww", 90);

    }

    /**
     * 位图
     */
    @Test
    void bitmapTest() {

        String bitamp = "test:bitmap:20200202";
        String bitamp3 = "test:bitmap:20200203";
        String bitamp4 = "test:bitmap:20200204";
        ValueOperations valueOperations = redisTemplate.opsForValue();

        //
        valueOperations.setBit(bitamp, 0, true);
        valueOperations.setBit(bitamp, 1, true);
        valueOperations.setBit(bitamp, 2, true);
        valueOperations.setBit(bitamp, 2, false);

        valueOperations.setBit(bitamp3, 0, true);
        valueOperations.setBit(bitamp3, 1, true);

        valueOperations.setBit(bitamp4, 1, true);
        valueOperations.setBit(bitamp4, 2, false);

        //查找
        System.out.println(valueOperations.getBit(bitamp, 2));

        //统计
        Object obj = redisTemplate.execute(new RedisCallback() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.bitCount(bitamp.getBytes());
            }
        });
        System.out.println(obj);


        //统计3组数据的布尔值，并对这3组数据做OR运算
        String redisKey = "test:bimap:or";
        Object or = redisTemplate.execute(new RedisCallback() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                connection.bitOp(RedisStringCommands.BitOperation.OR,
                        redisKey.getBytes(), bitamp.getBytes(), bitamp3.getBytes(), bitamp4.getBytes());
                return connection.bitCount(redisKey.getBytes());
            }
        });
        System.out.println(or);

    }

    /**
     * 基数
     */
    @Test
    void HyperLogLogTest() {
        String hyperLogLog = "test:hll:ids";
        HyperLogLogOperations<String, Integer> hyperLogLogOperations = redisTemplate.opsForHyperLogLog();
        hyperLogLogOperations.add(hyperLogLog, 1, 2, 3, 3, 4, 5);
        Long size = hyperLogLogOperations.size(hyperLogLog);
        System.out.println(size); //5

        //统计
        Object obj = redisTemplate.execute(new RedisCallback() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.hyperLogLogCommands().pfCount(hyperLogLog.getBytes());
            }
        });

    }

}
