package com.km.commons.redismq.api;


/**
 * 队列处理工具
 * 
 * @author KM
 *
 */
public abstract class RedisQueueProcesser extends RedisChannelProcesser {

  // 取得订阅的消息后的处理
  public final void doProcess(String name, String message) {
    boolean ok = false;
    try {
      doQueueProcesser(name, message);
      ok = true;
    } finally {
      infoPersisiten(name, message, ok);
    }
  }

  public abstract void doQueueProcesser(String name, String message);

}
