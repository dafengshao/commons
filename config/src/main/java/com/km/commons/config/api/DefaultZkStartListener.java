package com.km.commons.config.api;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.km.commons.config.core.ZKPropertyPlaceholderConfigurer;

public class DefaultZkStartListener implements ZKStartListener {
  private final Logger logger = LoggerFactory.getLogger(DefaultZkStartListener.class);

  // private static final int
  @Override
  public void executor(CuratorFramework curatorframework, ConnectionState newState) {
    if (newState == ConnectionState.RECONNECTED) {
      ZKPropertyPlaceholderConfigurer.reLoadConfigFromZK();
    }
    logger.info("com.km.commons.config.api.DefaultZkStartListener executor success.newState={}",
        newState);

  }
}
