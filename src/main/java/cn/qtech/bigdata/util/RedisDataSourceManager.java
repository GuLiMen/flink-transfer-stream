package cn.qtech.bigdata.util;

import cn.qtech.bigdata.comm.AppConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


public class RedisDataSourceManager implements Serializable {
    /* private static String host = PropertiesManager.getStrProperty("redis.host");
     private static int port = Integer.parseInt(PropertiesManager.getStrProperty("redis.port"));
     private static String password = PropertiesManager.getStrProperty("redis.password");*/
    private static String host = AppConstants.REIDS_HOST;
    private static int port = AppConstants.REIDS_PORT;
    private static String password = AppConstants.REDIS_PASSWD;


    private static final int REDIS_TIMEOUT = 60000; //客户端超时时间单位是毫秒
    private static final int REDIS_MAXTOTAL = 100;  //最大连接数
    private static final int REDIS_MAXIDLE = 20; //最大空闲数
    private static final int REDIS_MAXWAIT = 1000; //最大建立连接等待时间
    private static final boolean REDIS_TEST_ON_BORROW = true;

    private static final String masterName = "mymaster";


    private static final JedisSentinelPool jedisPool = new JedisSentinelPool(masterName, sentinels(), jedisPoolConfig(), REDIS_TIMEOUT, "redis20!&");

    //private static final JedisPool jedisPool = new JedisPool(jedisPoolConfig(), host, port, REDIS_TIMEOUT, password);
    // private static final JedisPool jedisPool = new JedisPool(jedisPoolConfig(), host, port, REDIS_TIMEOUT);

    private static final Logger LOG = LoggerFactory.getLogger(RedisDataSourceManager.class);

    private static JedisPoolConfig jedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(REDIS_MAXIDLE);
        jedisPoolConfig.setMaxTotal(REDIS_MAXTOTAL);
        jedisPoolConfig.setMaxWaitMillis(REDIS_MAXWAIT);
        jedisPoolConfig.setTestOnBorrow(REDIS_TEST_ON_BORROW);
        return jedisPoolConfig;
    }

    private static Set<String> sentinels() {
        HashSet<String> sentinelsSet = new HashSet<>();
        sentinelsSet.add("10.170.3.11:26379");
        sentinelsSet.add("10.170.3.12:26379");
        return sentinelsSet;
    }

    public  Jedis getResource() {
        return jedisPool.getResource();

    }

}
