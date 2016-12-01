package com.km.commons.redismq.api;

/** 消息队列持久化接口 */
public interface PersistentHandler {
  /** 队列，广播 **/
  public enum MessageType {
    queue, publish
  }
  /** 发生者，消费者 **/
  public enum RoleType {
    provider, customer
  }

  /**
   * 持久化消息接口
   * 
   * @param messageType 消息类型
   * @param roleType 角色类型
   * @param b 消息处理成功还是失败
   * @param channel 消息名称
   * @param message 消息内容
   */
  void handle(MessageType messageType, RoleType roleType, boolean b, String channel, String message);
}
