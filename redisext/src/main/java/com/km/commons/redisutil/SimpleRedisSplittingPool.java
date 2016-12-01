package com.km.commons.redisutil;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;

/**
 * Redis 读写分离 数据源. 获取线程安全的jedis代理<br>
 * jdk：InvocationHandler <br>
 * cglib MethodInterceptor
 */
public class SimpleRedisSplittingPool implements MethodInterceptor {

	private Pool<Jedis> readJedisPool;
	private Pool<Jedis> writeJedisPool;

	private static Jedis jedisProxy;

	public Jedis getResource() {
		if (jedisProxy == null) {
			synchronized (SimpleRedisSplittingPool.class) {
				if (jedisProxy == null) {
					Enhancer enhancer = new Enhancer();
					enhancer.setSuperclass(Jedis.class);
					enhancer.setCallback(this);
					jedisProxy = (Jedis) enhancer.create(new Class[] { String.class }, new Object[] { new String() });
				}
			}
		}
		return jedisProxy;
	}

	// cglib实现
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		String methodName = method.getName();
		if (methodName.equals("close")) {
			return null;
		}
		Pool<Jedis> pool = null;
		Jedis jedis = null;
		try {
			if (isReadCommand(methodName)) {
				pool = this.readJedisPool;
			} else {
				pool = this.writeJedisPool;
			}
			jedis = (Jedis) pool.getResource();
			return proxy.invoke(jedis, args);
		} finally {
			if ((pool != null) && (jedis != null)) {
				jedis.close();
			}
		}
	}

	private boolean isReadCommand(String methodName) {
		if (methodName.equals("getSet")) {
			return false;
		} else if ((methodName.indexOf("get") > -1) || ("hvals".equals(methodName)) || (methodName.indexOf("len") > -1)
				|| (methodName.indexOf("card") > -1)
				/**
				 * list/hash/set/zset length
				 */
				|| ("lindex".equals(methodName)) || ("lrange".equals(methodName)) || ("exists".equals(methodName))
				|| ("hexists".equals(methodName)) || ("keys".equals(methodName)) || ("hkeys".equals(methodName))
				|| ("ttl".equals(methodName)) || ("type".equals(methodName)) || ("zcount".equals(methodName))
				|| ("zscore".equals(methodName)) || (methodName.indexOf("zran") > -1)
				|| (methodName.indexOf("zrevran") > -1) || (methodName.indexOf("member") > -1)
				|| ("sdiff".equals(methodName)) || ("sinsert".equals(methodName))
				|| ("sunion".equals(methodName))/** set */
		) {
			return true;
		}
		return false;
	}

	public Pool<Jedis> getReadJedisPool() {
		return readJedisPool;
	}

	public void setReadJedisPool(Pool<Jedis> readJedisPool) {
		this.readJedisPool = readJedisPool;
	}

	public Pool<Jedis> getWriteJedisPool() {
		return writeJedisPool;
	}

	public void setWriteJedisPool(Pool<Jedis> writeJedisPool) {
		this.writeJedisPool = writeJedisPool;
	}

	public static void main(String[] args) {
		JedisPool writePool = new JedisPool("10.1.0.208", 6379);
		JedisPool readPool = new JedisPool("10.1.0.209", 6379);
		SimpleRedisSplittingPool pool = new SimpleRedisSplittingPool();
		pool.setReadJedisPool(readPool);
		pool.setWriteJedisPool(writePool);
		Jedis jedis = pool.getResource();

		String hwf = jedis.get("hwf");
		System.out.println(hwf);
		jedis.set("hwf", "newwfff");
		hwf = jedis.get("hwf");
		jedis.close();
		jedis.close();
		jedis.close();
		System.out.println(hwf);
	}
	// jdk 动态代理
	// public Object invoke(Object proxy, Method method, Object[] args) throws
	// Throwable {
	// String methodName = method.getName();
	// Pool<ShardedJedis> pool = null;
	// ShardedJedis jedis = null;
	// try {
	// if (isReadCommand(methodName)) {
	// pool = this.readShardedJedisPool;
	// } else {
	// pool = this.writeShardedJedisPool;
	// }
	// jedis = (ShardedJedis) pool.getResource();
	// method.invoke(proxy, args);
	// } finally {
	// if ((pool != null) && (jedis != null)) {
	// // pool.returnResource(jedis);
	// jedis.close();
	// }
	// }
	// return null;
	// }

}
