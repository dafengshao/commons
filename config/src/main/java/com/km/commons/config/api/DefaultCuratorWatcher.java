package com.km.commons.config.api;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.km.commons.config.core.Configuration;
import com.km.commons.config.core.CuratorFrameworkZKClientFactory;

public class DefaultCuratorWatcher implements org.apache.curator.framework.api.CuratorWatcher {
  private static CuratorFramework zkClient = CuratorFrameworkZKClientFactory.getZkClient();
  private final Logger logger = LoggerFactory.getLogger(DefaultCuratorWatcher.class);

  @Override
  public void process(WatchedEvent event) throws Exception {
    String path = event.getPath();
    if (path == null) {
      return;
    }
    try {
      byte[] data = zkClient.getData().forPath(path);
      ByteArrayInputStream in = new ByteArrayInputStream(data, 0, data.length);
      Properties props = new Properties();
      props.load(in);
      Configuration.buildConfig(props);
      logger.info("reload 配置 {} success.", path);
    } finally {
      zkClient.getData().usingWatcher(this).inBackground().forPath(path);
    }
  }
}
