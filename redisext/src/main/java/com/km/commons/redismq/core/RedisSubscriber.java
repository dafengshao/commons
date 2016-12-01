package com.km.commons.redismq.core;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.util.Pool;

import com.km.commons.redismq.api.RedisChannelProcesser;

/**
 * reids 订阅工具
 * 
 * @author KM
 *
 */
public class RedisSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(RedisQueueListener.class);
  private Pool<Jedis> redisPool;
  private static ExecutorService executorService;
  private List<RedisChannelProcesser> redisHanlders;

  @SuppressWarnings("unused")
  private void init() {
    if (redisHanlders == null) {
      return;
    }
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(1);
    }
    for (RedisChannelProcesser redisHanlder : redisHanlders) {
      MyWorker myWorker = new MyWorker(redisHanlder);
      executorService.execute(myWorker);
      LOG.info("channel:{}订阅成功", redisHanlder.getName());
    }
  }

  public void setRedisHanlders(List<RedisChannelProcesser> redisHanlders) {
    this.redisHanlders = redisHanlders;
  }

  public void setRedisPool(Pool<Jedis> redisPool) {
    this.redisPool = redisPool;
  }

  private class MyWorker implements Runnable {
    private final RedisChannelProcesser redisHanlder;

    public MyWorker(RedisChannelProcesser queue) {
      this.redisHanlder = queue;
    }

    @Override
    public void run() {
      Jedis jedis = null;
      try {
        jedis = redisPool.getResource();
        String channel = redisHanlder.getName();
        JedisPubSub jedisPubSub = (JedisPubSub) redisHanlder;
        jedis.subscribe(jedisPubSub, channel.split(","));
      } finally {
        if (jedis != null) {
          jedis.close();
        }
      }
    }
  }

  public void destroy() {
    shutdownAndAwaitTermination(executorService);
  }

  private void shutdownAndAwaitTermination(ExecutorService pool) {
    pool.shutdown();
    try {
      if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
        pool.shutdownNow();
        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
          LOG.error("Pool did not terminate");
        }
      }
    } catch (InterruptedException ie) {
      pool.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public static ExecutorService getExecutorService() {
    return executorService;
  }

  public static void setExecutorService(ExecutorService executorService) {
    RedisSubscriber.executorService = executorService;
  }



}
