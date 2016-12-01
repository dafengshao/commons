package com.km.commons.redismq.api;

/**
 * 消息处理工具
 * 
 * @author KM
 *
 */
public interface RedisProcesser {

  void doProcess(String key, String content);

}
