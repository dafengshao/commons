package com.km.commons.redisutil;



import java.lang.reflect.Method;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.InvocationHandler;
import net.sf.cglib.proxy.Proxy;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisProxy implements InvocationHandler {
	
		private JedisPool pool;

		public RedisProxy(JedisPool pool) {
			super();
			this.pool = pool;
		}

		/*
		 * public static boolean set(String key, String value) { Jedis jedis = null;
		 * try { jedis = pool.getResource(); return "OK".equals(jedis.set(key,
		 * value)); } catch (Exception e) { e.printStackTrace(); return false; }
		 * finally { if (jedis != null) { jedis.close(); } } }
		 * 
		 * public static String get(String key) { Jedis jedis = null; try { jedis =
		 * pool.getResource(); return jedis.get(key); } catch (Exception e) {
		 * e.printStackTrace(); return null; } finally { if (jedis != null) {
		 * jedis.close(); } } }
		 * 
		 * public static boolean zadd(String key, Double score, String member) {
		 * Jedis jedis = null; try { jedis = pool.getResource(); long zaddRe =
		 * jedis.zadd(key, score, member); return zaddRe == 1L; } catch (Exception
		 * e) { e.printStackTrace(); return false; } finally { if (jedis != null) {
		 * jedis.close(); } } }
		 */

		public static void main(String[] args) {
			JedisPoolConfig config = new JedisPoolConfig();
			JedisPool pool = new JedisPool(config, "127.0.0.1", 6379);
			Jedis jedis = (Jedis) new RedisProxy(pool).getProxy();
			String set = jedis.set("hwf","12","nx","ex",100);
			System.out.println(set);
		}

		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			if (method.getName().equals("close")) {
				return null;
			}
			if(method.getName().equals("toString")){
				return this.toString();
			}
			//System.out.println("jedis method name:"+method.getName());
			Jedis jedis = null;
			try {
				jedis = pool.getResource();
				//System.out.println("get jedis invoke:"+jedis);
				Object result = method.invoke(jedis, args);
				return result;
			} finally {
				if (jedis != null) {
					System.out.println("close jedis:"+jedis);
					jedis.close();
				}
			}
		}

		public Jedis getProxy() {
			Enhancer enhancer = new Enhancer();
			enhancer.setSuperclass(Jedis.class);
			enhancer.setCallback(this);
			Jedis jedisProxy = (Jedis) enhancer.create(new Class[] { String.class }, new Object[] { new String() });
			return jedisProxy;
		}

}
