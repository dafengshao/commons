package redisext.test;

import java.util.Date;
import java.util.Random;

import com.km.commons.redismq.core.RedisMessageSender;
import com.km.commons.redisutil.RedisLocker;
import com.km.commons.redisutil.RedisLocker.OwnerLock;
import com.km.commons.redisutil.SimpleRedisSplittingPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;

public class PromotionRedisHanlder {

	public static void main(String[] args) throws InterruptedException {

		SimpleRedisSplittingPool pool = new SimpleRedisSplittingPool();
		Pool<Jedis> jedisPool = new JedisPool("10.1.0.208", 6379);
		pool.setReadJedisPool(new JedisPool("10.1.0.209", 6379));
		pool.setWriteJedisPool(jedisPool);
		RedisLocker redisLocker = new RedisLocker(pool);
		OwnerLock lock = redisLocker.lock("hwf111", 1);
		if (lock != null) {
			System.out.println("lock=OK:hwf=" + lock.getValue());
			// boolean release = lock.release();
			// System.out.println(release);
		} else {
			System.out.println("lock=false");
		}

		final RedisMessageSender sender = new RedisMessageSender();
		sender.setRedisPool(jedisPool);
		final Random r = new Random();
		int i = 0;
		while (i++ < 100) {
			Thread t = new Thread() {
				@Override
				public void run() {
					try {
						Thread.currentThread().sleep(r.nextInt(300));
					} catch (InterruptedException e) {
					}
					sender.send("queue_promotion", new Date().getTime() + "");
				}
			};
			t.start();
		}

	}
}
