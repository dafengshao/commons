package com.km.commons.redismq.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import com.km.commons.redismq.api.PersistentHandler;

/**
 * redis发布工具
 * 
 * @author KM
 *
 */
public class RedisMessageSender {
  private Pool<Jedis> redisPool;
  private static final Logger LOG = LoggerFactory.getLogger(RedisMessageSender.class);
  private PersistentHandler persistentHandler;

  public void setRedisPool(Pool<Jedis> redisPool) {
    this.redisPool = redisPool;
  }


  public void setPersistentHandler(PersistentHandler persistentHandler) {
    this.persistentHandler = persistentHandler;
  }

  /** 发布消息到指定渠道 */
  public Long publish(String channel, String message) {
    Jedis jedis = null;
    Long publishNum = -1L;
    try {
      jedis = redisPool.getResource();
      publishNum = jedis.publish(channel, message);
      LOG.debug("publish sesscess.channel:{},message:{} ,publish-num{}.", channel, message,
          publishNum);
    } finally {
      if (jedis != null) {
        jedis.close();
      }
      if (persistentHandler != null) {
        persistentHandler.handle(PersistentHandler.MessageType.publish,
            PersistentHandler.RoleType.provider, publishNum >= 0, channel, message);
      }
    }
    return publishNum;
  }

  /** 发送消息到指定队列 */
  public boolean send(String queueName, String content) {
    Jedis jedis = null;
    Long publishNum = -1L;
    try {
      jedis = redisPool.getResource();
      publishNum = jedis.lpush(queueName, content);
      return publishNum > 0;
    } finally {
      if (null != jedis) {
        jedis.close();
      }
      if (persistentHandler != null) {
        persistentHandler.handle(PersistentHandler.MessageType.queue,
            PersistentHandler.RoleType.provider, publishNum > 0, queueName, content);
      }
    }
  }
}
