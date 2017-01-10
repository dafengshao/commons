package com.km.commons.redisutil;

import java.util.UUID;

import redis.clients.jedis.Jedis;

public class RedisLocker {
	private static final String LOCKKEY_PREFIX = "lock:";
	private Jedis jedis;
	//private static final String SETNX_SCRIPT = "local key = KEYS[1] local ttl = KEYS[2]  local content = KEYS[3] local lockSet = redis.call('setnx', key, content)  if lockSet == 1 then redis.call('expire', key, ttl) end return lockSet ";
	private static final String DELANDEDQUAL_SCRIPT = "local key = KEYS[1] local value = KEYS[2] local rel = 0 local lockValue = redis.call('get', key) if lockValue == value then rel = redis.call('del', key) end return rel";

	//private static String SETNX_SCRIPT_SHA;
	private static String DELANDEDQUAL_SCRIPT_SHA;
	//10分钟 发生死锁后多长时间自动恢复
	private static int DEFAUL_LOCKTIME = 60 * 10;

	/*public RedisLocker(SimpleRedisSplittingPool redisPool) {
		this.jedis = redisPool.getResource();
		init();
	}*/
	/**locktime发生死锁后多长时间自动恢复 单位s*/
	public RedisLocker(RedisProxy jedis,int locktime) {
		if(locktime>0){
			DEFAUL_LOCKTIME = locktime;
		}
		this.jedis = jedis.getProxy();
		init();
	}
	public void init(){
		//SETNX_SCRIPT_SHA = this.jedis.scriptLoad(SETNX_SCRIPT);// 62aa9a2b3ba1ebbfffc14d2a64c97a9a973337ed
		DELANDEDQUAL_SCRIPT_SHA = this.jedis.scriptLoad(DELANDEDQUAL_SCRIPT);// ab877167417583827bd6ab2116eb747ae0ad07e5
	}
	/** 不存在就设值，并且设置过期时间，原子操作 */
	public boolean setNxEx(String key, String value, long seconds) {
		String set = jedis.set(key, value, "nx", "ex", seconds);
		//long a = ((Long) jedis.evalsha(SETNX_SCRIPT_SHA, 3, key, seconds + "", value)).longValue();
		return "OK".equals(set);

	}

	/**
	 * 给指定参数加锁，需要调用release释放 *
	 * 
	 * @param key
	 *            锁定的key
	 * @param timeOut
	 *            单位ms，如果已经被加锁，需等待的时间，等待超时将返回失败
	 * @return null失败，OwnerLock成功，返回结果需要在释放时使用
	 * @throws InterruptedException
	 */
	public OwnerLock tryLock(String key, long timeOut) {
		long waitEndTime = System.currentTimeMillis() + (timeOut);
		OwnerLock lock = null;
		while((lock = this.tryLock(key))==null){
			long currTime = System.currentTimeMillis();
			if (waitEndTime < currTime) {// 加锁失败 等待超时
				// LOG.error("key:{}加锁失败,等待超时!", key);
				return null;
			}
			try {
				Thread.sleep(101);
			} catch (InterruptedException e) {
				
			}
		}
		return lock;
	}

	/**
	 * @param key
	 *            指定参数加锁，需要调用release释放
	 * 
	 * @return null失败，OwnerLock成功，返回结果需要在释放时使用
	 */
	public OwnerLock tryLock(String key) {
		return lock(key, DEFAUL_LOCKTIME);
	}

	/** 给指定参数加锁,lockSeconds为锁定时间单位秒，需要调用release释放 */
	protected OwnerLock lock(String key, long lockSeconds) {
		String lockKey = (LOCKKEY_PREFIX.concat(key));
		UUID uuid = UUID.randomUUID();
		OwnerLock lock = null;
		if (this.setNxEx(lockKey, uuid.toString(), lockSeconds)) {
			lock = new OwnerLock(lockKey, uuid.toString());
		}
		uuid = null;
		return lock;
	}

	/**
	 * 释放key,这个方法在加锁成功锁使用完毕以后调用，需要放到finally里
	 * 
	 * @param key
	 * @return
	 */
	public boolean release(OwnerLock ownerLock) {
		if (ownerLock == null) {
			return false;
		}
		int i = 0;
		while(i++<10){
			try{
				Long delOk = (Long) jedis.evalsha(DELANDEDQUAL_SCRIPT_SHA, 2, ownerLock.getKey(), ownerLock.getValue());
				boolean ok = delOk.longValue() == 1;
				if(ok){
					return true;
				}else{
					return false;
				}
				
			}catch(Exception e){
				try {
					Thread.sleep(101);
				} catch (InterruptedException e1) {
					
				}
			}
			
		}
		return false;
		
	}

	public class OwnerLock {
		private String key;
		private String value;

		public OwnerLock(String key, String value) {
			this.key = key;
			this.value = value;

		}

		public boolean release() {
			return RedisLocker.this.release(this);
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
		@Override
		public String toString() {
			return super.toString()+"{key:"+key+",value:"+value+"}";
		}

	}

}
