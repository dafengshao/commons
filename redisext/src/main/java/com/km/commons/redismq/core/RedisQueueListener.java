package com.km.commons.redismq.core;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

import com.km.commons.redismq.api.RedisChannelProcesser;

/**
 * Redis队列监听器.
 */
public class RedisQueueListener {
  private static final Logger LOG = LoggerFactory.getLogger(RedisQueueListener.class);

  private static ExecutorService executorService;

  private Pool<Jedis> redisPool;
  private List<RedisChannelProcesser> redisHanlders;

  public void setRedisPool(Pool<Jedis> redisPool) {
    this.redisPool = redisPool;
  }

  public void setRedisHanlders(List<RedisChannelProcesser> redisHanlders) {
    this.redisHanlders = redisHanlders;
  }

  @SuppressWarnings("unused")
  private void init() {
    if (redisHanlders == null) {
      return;
    }
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(1);
    }
    for (RedisChannelProcesser queue : redisHanlders) {
      Worker worker = new Worker(queue);
      executorService.execute(worker);
      LOG.info("队列名称为：{}，开始监听", queue.getName());
    }
  }

  public void destory() {
    shutdownAndAwaitTermination(executorService);
  }

  private class Worker implements Runnable {
    private final RedisChannelProcesser redisHanlder;

    public Worker(RedisChannelProcesser queue) {
      this.redisHanlder = queue;
    }

    @Override
    public void run() {
      String name = redisHanlder.getName();
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Jedis jedis = null;
          try {
            jedis = redisPool.getResource();
            List<String> datas = jedis.brpop(3, name);
            if ((datas != null) && (datas.size() == 2)) {
              try {
                String keyname = datas.get(0);
                String content = datas.get(1);
                LOG.info("从redis队列中取出的数据为：queueName={},content={}", keyname, content);
                if (redisHanlder != null) {
                  redisHanlder.doProcess(name, content);
                }
              } catch (Exception e) {
                LOG.error("从redis队列处理消息出现异常：queueName：{},content={}", datas.get(0), datas.get(1), e);
              }
            }
          } finally {
            if (null != jedis) {
              jedis.close();
            }
          }
        }
      } catch (JedisConnectionException e) {
        LOG.error("队列名称为：{}，监听异常", name, e);
      }
    }
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
    RedisQueueListener.executorService = executorService;
  }


}
