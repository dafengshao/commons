package com.km.commons.redisutil;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

/**
 * Redis 工具类  可以用spring工具代替
 */
@Deprecated
public class RedisTemplate {


  // private static final Logger LOG = LoggerFactory.getLogger(RedisTemplate.class);
  private Pool<ShardedJedis> readShardedJedisPool;
  private Pool<ShardedJedis> writeShardedJedisPool;



  public boolean exists(String key) {
    ShardedJedis jedis = readShardedJedisPool.getResource();
    boolean result = false;
    try {
      result = jedis.exists(key);
    } catch (JedisConnectionException e) {
      if (null != jedis) {
        readShardedJedisPool.returnBrokenResource(jedis);
        jedis = null;
      }
    } finally {
      if (null != jedis) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
    return result;
  }

  public boolean expire(String key, int seconds) {
    ShardedJedis jedis = null;
    Long ex = 0L;
    try {
      jedis = writeShardedJedisPool.getResource();
      ex = jedis.expire(key, seconds);
    } finally {
      if (null != jedis) {
        writeShardedJedisPool.returnBrokenResource(jedis);
        jedis = null;
      }
    }
    return ex.intValue() == 1;
  }

  public boolean expireAt(String key, long expireAt) {
    int se = Long.valueOf((expireAt - System.currentTimeMillis()) / 1000).intValue();
    return expire(key, se);
  }

  public void push(String queueName, String content) {
    ShardedJedis jedis = writeShardedJedisPool.getResource();
    try {
      jedis.lpush(queueName, content);
    } catch (JedisConnectionException e) {
      if (null != jedis) {
        writeShardedJedisPool.returnBrokenResource(jedis);
        jedis = null;
      }
    } finally {
      if (null != jedis) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public void set(String key, String value) {
    ShardedJedis jedis = writeShardedJedisPool.getResource();
    try {
      jedis.set(key, value);
    } catch (JedisConnectionException e) {
      if (null != jedis) {
        writeShardedJedisPool.returnBrokenResource(jedis);
        jedis = null;
      }
    } finally {
      if (null != jedis) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public void setex(String key, String value, int timeout) {
    ShardedJedis jedis = writeShardedJedisPool.getResource();
    try {
      jedis.setex(key, timeout, value);
    } catch (JedisConnectionException e) {
      if (null != jedis) {
        writeShardedJedisPool.returnBrokenResource(jedis);
        jedis = null;
      }
    } finally {
      if (null != jedis) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public String get(String key) {
    ShardedJedis jedis = readShardedJedisPool.getResource();
    String value = "";
    try {
      value = jedis.get(key);
    } catch (JedisConnectionException e) {
      if (null != jedis) {
        readShardedJedisPool.returnBrokenResource(jedis);
        jedis = null;
      }
    } finally {
      if (null != jedis) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
    return value;
  }

  public String getAndDel(String key) {
    ShardedJedis jedis = writeShardedJedisPool.getResource();
    String value = "";
    try {
      value = jedis.get(key);
      jedis.del(key);
    } catch (JedisConnectionException e) {
      if (null != jedis) {
        writeShardedJedisPool.returnBrokenResource(jedis);
        jedis = null;
      }
    } finally {
      if (null != jedis) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
    return value;
  }

  public void del(String key) {
    ShardedJedis jedis = writeShardedJedisPool.getResource();
    try {
      jedis.del(key);
    } catch (JedisConnectionException e) {
      if (null != jedis) {
        writeShardedJedisPool.returnBrokenResource(jedis);
        jedis = null;
      }
    } finally {
      if (null != jedis) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public List<String> lRange(String key) {
    List<String> result = null;
    ShardedJedis jedis = readShardedJedisPool.getResource();
    try {
      result = jedis.lrange(key, 0, -1);
      return result;
    } finally {
      if (null != jedis) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  /**
   * @param key
   * @param value
   * @return
   */
  public long setnx(String key, String value) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      long setnxSuccess = jedis.setnx(key, value);
      return setnxSuccess;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  private boolean setLockNx(String key, int lockSeonds) {
    ShardedJedis jedis = null;
    try {
      if (lockSeonds <= 0) {
        throw new RuntimeException("lockSeonds 必须大于 0");
        // return false;
      }
      Long valueLong = System.currentTimeMillis() + lockSeonds * 1000L + 1;
      jedis = writeShardedJedisPool.getResource();
      long setnxSuccess = jedis.setnx(key, valueLong.toString());
      if (setnxSuccess == 1) {
        long exSuccess = jedis.expire(key, lockSeonds);
        if (exSuccess == 1) {
          return true;
        }
      } else {
    	  
      }
      return false;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  private static String setNxScript1 = "local key = KEYS[1] local ttl = KEYS[2] "
      + "local content = KEYS[3] local lockSet = redis.call('setnx', key, content) "
      + "if lockSet == 1 then redis.call('expire', key, ttl) end return lockSet ";

  // static String setNxScript2 =
  // "local key = KEYS[1] local ttl = KEYS[2] local content = KEYS[3] local lockExpire =1 local lockSet = redis.call('setnx', key, content) if lockSet == 1 then lockExpire = redis.call('pexpire', key, ttl) end if lockSet == 1 and lockExpire == 0 then redis.call('del', key) end return lockSet+lockExpire ";
  public boolean setNxEx(String key, String value, long seconds) {
    Jedis jedis = null;
    ShardedJedis shardedJedis = null;
    try {
      shardedJedis = writeShardedJedisPool.getResource();
      jedis = shardedJedis.getShard(key);
      long a = ((Long) jedis.eval(setNxScript1, 3, key, seconds + "", value)).longValue();
      // shardedJedis.
      return a == 1L;
    } finally {
      if (jedis != null) {
        jedis.close();
      }
      if (shardedJedis != null) {
        shardedJedis.close();
      }
    }
  }

  public boolean lock(String key, int timeOut) {
    long waitEndTime = System.currentTimeMillis() + (timeOut * 1000);
    String lockKey = ("JedisLock_".concat(key)).intern();
    while (!this.setNxEx(lockKey, "1", 60 * 60 * 3)) {
      long currTime = System.currentTimeMillis();
      if (waitEndTime < currTime) {// 鍔犻攣澶辫触 绛夊緟瓒呮椂
        // LOG.error("key:{}鍔犻攣澶辫触,绛夊緟瓒呮椂!", key);
        return false;
      }
      try {
        Thread.sleep(150);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return true;
  }

  public boolean tryLock(String key) {
    String lockKey = ("JedisLock_".concat(key)).intern();
    return this.setNxEx(lockKey, "1", 60 * 60 * 3);
  }

  protected boolean tryLock(String key, long lockSeconds) {
    String lockKey = ("JedisLock_".concat(key)).intern();
    return this.setNxEx(lockKey, "1", lockSeconds);
  }

  /**
   * 
   * @param key
   * @return
   */
  public boolean release(String key) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      String lockKey = ("JedisLock_".concat(key)).intern();
      boolean ok = jedis.del(lockKey) == 1;
      return ok;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public List<String> mget(String... keys) {
    if (keys == null || keys.length <= 0) {
      return null;
    }
    List<String> result = null;
    ShardedJedis sjedis = null;
    Jedis jedis = null;
    try {
      sjedis = readShardedJedisPool.getResource();
      jedis = sjedis.getShard(keys[0]);
      result = jedis.mget(keys);
      return result;
    } finally {
      if (null != sjedis) {
        sjedis.close();
      }
      if (null != jedis) {
        jedis.close();
      }
    }

  }

  public static String getLockKey(String key) {
    return ("JedisLock_" + key).intern();
  }

  public long incr(String key) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      long result = jedis.incr(key);
      return result;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }

  }

  public long decr(String key) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      long result = jedis.decr(key);
      return result;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }

  }

  public void zadd(String key, double socre, String value) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      jedis.zadd(key, socre, value);
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public Set<String> zrange(String key, long start, long end) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.zrange(key, start, end);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public Set<String> zrangeByScore(String key, double min, double max) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.zrangeByScore(key, min, max);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  /**
   * 鑾峰彇鏈夊簭闆嗗悎 浠巗tart鍒癳nd鏉�鍗囧簭 闄や簡鎴愬憳鎸�score 鍊奸�鍑忕殑娆″簭鎺掑垪杩欎竴鐐瑰锛�ZREVRANGE 鍛戒护鐨勫叾浠栨柟闈㈠拰 ZRANGE 鍛戒护涓�牱銆�
   * */
  public Set<String> zrevrangeByScore(String key, double min, double max) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.zrevrangeByScore(key, max, min);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public Set<String> zrevrange(String key, long start, long end) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.zrevrange(key, start, end);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public void setexat(byte[] key, byte[] value, long expireAt) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      jedis.set(key, value);
      if (expireAt > 0) {
        int se = Long.valueOf((expireAt - System.currentTimeMillis()) / 1000).intValue();
        jedis.expire(key, se);
      }
      return;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public void setexat(String key, String value, long expireAt) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      jedis.set(key, value);
      if (expireAt > 0) {
        int se = Long.valueOf((expireAt - System.currentTimeMillis()) / 1000).intValue();
        jedis.expire(key, se);
      }
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public byte[] get(byte[] key) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.get(key);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public void hmset(String key, Map<String, String> value) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      jedis.hmset(key, value);
      return;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public void hmset(String key, Map<String, String> value, long expireAt) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      // jedis.
      jedis.hmset(key, value);
      if (expireAt > 0) {
        int se = Long.valueOf((expireAt - System.currentTimeMillis()) / 1000).intValue();
        jedis.expire(key, se);
      }
      return;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public String hget(String key, String field) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.hget(key, field);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public Long hset(String key, String field, String value) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      return jedis.hset(key, field, value);
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public List<String> hmget(String key, String[] fields) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.hmget(key, fields);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public Map<String, String> hgetAll(String key) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.hgetAll(key);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public void sadd(String key, long expireAt, String... members) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      jedis.sadd(key, members);
      if (expireAt > 0) {
        int se = Long.valueOf((expireAt - System.currentTimeMillis()) / 1000).intValue();
        jedis.expire(key, se);
      }
      return;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }



  public boolean sismember(String key, String member) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.sismember(key, member);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public Set<String> smembers(String key) {
    ShardedJedis jedis = null;
    try {
      jedis = readShardedJedisPool.getResource();
      return jedis.smembers(key);
    } finally {
      if (jedis != null) {
        readShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public void zaddex(String key, long expireAt, Map<String, Double> skuIds) {
    ShardedJedis jedis = null;
    try {
      jedis = writeShardedJedisPool.getResource();
      jedis.zadd(key, skuIds);
      // jedis.zadd(key, scoreMembers)
      int se = Long.valueOf((expireAt - System.currentTimeMillis()) / 1000).intValue();
      jedis.expire(key, se);
      return;
    } finally {
      if (jedis != null) {
        writeShardedJedisPool.returnResource(jedis);
      }
    }
  }

  public Pool<ShardedJedis> getReadShardedJedisPool() {
    return readShardedJedisPool;
  }

  public void setReadShardedJedisPool(Pool<ShardedJedis> readShardedJedisPool) {
    this.readShardedJedisPool = readShardedJedisPool;
  }

  public Pool<ShardedJedis> getWriteShardedJedisPool() {
    return writeShardedJedisPool;
  }

  public void setWriteShardedJedisPool(Pool<ShardedJedis> writeShardedJedisPool) {
    this.writeShardedJedisPool = writeShardedJedisPool;
  }

  public ShardedJedis getReadJedis() {
    return this.readShardedJedisPool.getResource();
  }

  public ShardedJedis getWriteJedis() {
    return this.writeShardedJedisPool.getResource();
  }

  public void returnReadResource(ShardedJedis jedis) {
    if (jedis != null) {
      this.readShardedJedisPool.returnResource(jedis);
    }
  }

  public void returnWriteResource(ShardedJedis jedis) {
    if (jedis != null) {
      this.writeShardedJedisPool.returnResource(jedis);
    }
  }

  // private static ShardedJedis jedisProxy;
  //
  // public ShardedJedis getResource() {
  // if (jedisProxy == null) {
  // synchronized (RedisTemplate.class) {
  // if (jedisProxy == null) {
  // Enhancer enhancer = new Enhancer();
  // enhancer.setSuperclass(ShardedJedis.class);
  // enhancer.setCallback(this);
  // jedisProxy =
  // (ShardedJedis) enhancer.create(new Class[] {Object.class},
  // new Object[] {new Object()});
  // }
  // }
  // }
  //
  // return jedisProxy;
  // }
  //
  // // cglib瀹炵幇
  // public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy)
  // throws Throwable {
  // String methodName = method.getName();
  // // String poolName = null;
  // Pool<ShardedJedis> pool = null;
  // ShardedJedis jedis = null;
  // try {
  // if ((methodName.indexOf("get") > -1) || ("smembers".equals(methodName))
  // || ("type".equals(methodName)) || ("exists".equals(methodName))) {
  // pool = this.readShardedJedisPool;
  // } else {
  // pool = this.writeShardedJedisPool;
  // }
  // // jedis.get
  // jedis = (ShardedJedis) pool.getResource();
  // return proxy.invoke(jedis, args);
  // } finally {
  // if ((pool != null) && (jedis != null)) {
  // jedis.close();
  // }
  // }
  // }
  //
  // private boolean isReadCommand(String methodName) {
  // return false;
  // }
  //
  // // jdk 鍔ㄦ�浠ｇ悊
  // public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
  // String methodName = method.getName();
  // // String poolName = null;
  // Pool<ShardedJedis> pool = null;
  // ShardedJedis jedis = null;
  // try {
  // if (methodName.equals("getSet")) {
  // pool = this.writeShardedJedisPool;
  // } else if ((methodName.indexOf("get") > -1) || ("smembers".equals(methodName))
  // || ("type".equals(methodName)) || ("exists".equals(methodName))) {
  // pool = this.readShardedJedisPool;
  // } else {
  // pool = this.writeShardedJedisPool;
  // }
  // jedis = (ShardedJedis) pool.getResource();
  // // jedis.getSet(key, value)
  // method.invoke(proxy, args);
  // } finally {
  // if ((pool != null) && (jedis != null)) {
  // jedis.close();
  // }
  // }
  // return null;
  // }
}
