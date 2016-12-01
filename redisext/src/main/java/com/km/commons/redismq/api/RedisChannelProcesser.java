package com.km.commons.redismq.api;

import redis.clients.jedis.JedisPubSub;

/**
 * 队列、订阅处理工具
 * 
 * @author hwf
 *
 */
public abstract class RedisChannelProcesser extends JedisPubSub implements RedisProcesser {
  protected PersistentHandler persistentHandler;
  /** redis对应的key，队列的时候为队列名，发布订阅的时候为channel名 **/
  protected String name;

  /** redis对应的key，队列的时候为队列名，发布订阅的时候为channel名 **/
  public void setName(String name) {
    this.name = name;
  }

  /** redis对应的key，队列的时候为队列名，发布订阅的时候为channel名 **/
  public String getName() {
    return name;
  }

  // 取得订阅的消息后的处理
  public void onMessage(String channel, String message) {
    boolean ok = false;
    try {
      doProcess(channel, message);
      ok = true;
    } finally {
      infoPersisiten(channel, message, ok);
    }
  }

  protected void infoPersisiten(String channel, String message, boolean ok) {
    if (persistentHandler != null) {
      persistentHandler.handle(PersistentHandler.MessageType.publish,
          PersistentHandler.RoleType.customer, ok, channel, message);
    }
  }

  // 初始化订阅时候的处理
  public void onSubscribe(String channel, int subscribedChannels) {
    // System.out.println(channel + "=" + subscribedChannels);
  }

  // 取消订阅时候的处理
  public void onUnsubscribe(String channel, int subscribedChannels) {
    // System.out.println(channel + "=" + subscribedChannels);
  }

  // 初始化按表达式的方式订阅时候的处理
  public void onPSubscribe(String pattern, int subscribedChannels) {
    // System.out.println(pattern + "=" + subscribedChannels);
  }

  // 取消按表达式的方式订阅时候的处理
  public void onPUnsubscribe(String pattern, int subscribedChannels) {
    // System.out.println(pattern + "=" + subscribedChannels);
  }

  // 取得按表达式的方式订阅的消息后的处理
  public void onPMessage(String pattern, String channel, String message) {
    // System.out.println(pattern + "=" + channel + "=" + message);
  }

  public void setPersistentHandler(PersistentHandler persistentHandler) {
    this.persistentHandler = persistentHandler;
  }
}
