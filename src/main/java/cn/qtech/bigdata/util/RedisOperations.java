package cn.qtech.bigdata.util;

import redis.clients.jedis.Jedis;

import java.io.Serializable;

public  class RedisOperations implements Serializable {
    RedisDataSourceManager jedisPool = new RedisDataSourceManager();

    /**
     * 通过Redis的key获取值
     * @param key
     * @return
     */
    public String get(String key){
        Jedis jedis =null;
        String value =null;
        try {
            jedis = jedisPool.getResource();
            value = jedis.get(key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return value;
    }

    /**
     *向redis存入key和value
     * @param key
     * @param value
     * @return
     */
    public String set(String key, String value){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.set(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != jedis) {
                jedis.close();

            }
        }
        return "0";
    }

    /**
     * 向redis存入key和value,设置某个key的超时时间（单位:秒）
     * @param key
     * @param seconds
     * @param value
     * @return
     */
    public String setex(String key,int seconds,String value){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setex(key,seconds,value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return "0";
    }
    /**
     * 设置某个key的超时时间（单位:秒）
     * @param key
     * @param seconds
     * @return
     */
    public Long expire(String key,int seconds){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return 0L;
    }

    /**
     * 通过Redis的key field 获取值
     * @param key
     * @return
     */
    public String hget(String key,String field){
        Jedis jedis =null;
        String value =null;
        try {
            jedis = jedisPool.getResource();
            value= jedis.hget(key,field);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return value;
    }

    /**
     * 通过Redis的key field 获取值
     * @param key
     * @return
     */
    public Long hset(String key,String field,String value){
        Jedis jedis =null;
        try {
            jedis = jedisPool.getResource();
           return jedis.hset(key, field, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return 0L;
    }


    /**
     * 删除key
     * @param key
     * @return
     */
    public Long delete(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.del(key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return 0L;
    }

}
