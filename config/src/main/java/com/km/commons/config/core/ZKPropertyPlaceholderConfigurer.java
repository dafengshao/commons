package com.km.commons.config.core;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

/**
 * SPRING启动时从ZK集群加载配置文件
 * 
 * @author river
 * 
 */
public class ZKPropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer {

  private static final Logger logger = LoggerFactory
      .getLogger(ZKPropertyPlaceholderConfigurer.class);

  // ZooKeeper 客户端
  private static CuratorFramework zkClient;

  // 配置文件存放节点路径
  private static List<String> zkPaths;

  private static CuratorWatcher watcher;

  @Override
  protected void processProperties(ConfigurableListableBeanFactory beanFactoryToProcess,
      Properties props) throws BeansException {

    // 保存本地配置信息
    for (Object key : props.keySet()) {
      String keyStr = key.toString();
      String value = props.getProperty(keyStr);
      Configuration.put(keyStr, value);
    }
    if (zkPaths != null) {
      zkClient = CuratorFrameworkZKClientFactory.getZkClient();
      if (null == zkClient) {
        logger.error("无法初始化zk客户端，加载ZooKeeper集群" + zkPaths + "节点配置信息失败！");
        throw new RuntimeException("无法初始化zk客户端，加载ZooKeeper集群" + zkPaths + "节点配置信息失败！");
      }
      loadConfigFromZK(props, zkPaths, watcher);
    }

    // 执行 SPRING 后续工作
    super.processProperties(beanFactoryToProcess, props);
  }


  /** 断开后重连 重新加载配置 */
  public static void reLoadConfigFromZK() {
    loadConfigFromZK(new Properties(), zkPaths, watcher);
  }

  public static void loadConfigFromZK(Properties props, List<String> paths, CuratorWatcher wathcher) {
    for (String path : paths) {
      try {
        Stat stat = zkClient.checkExists().forPath(path);
        if (null != stat) {
          byte[] data = zkClient.getData().forPath(path);
          ByteArrayInputStream in = new ByteArrayInputStream(data, 0, data.length);
          props.load(in);
          logger.info("加载ZK:{},{}配置文件成功", Configuration.getString("zk.host"), path);
          if (watcher != null) {
            zkClient.getData().usingWatcher(watcher).inBackground().forPath(path);
            logger.info("ZK集群中{}配置文件添加Watcher成功", path);
          }
        } else {
          logger.warn("ZK集群不存在{}这一路径的节点", path);
        }
      } catch (Exception e) {
        logger.error("加载ZooKeeper集群{}节点配置信息失败！", path, e);
        throw new RuntimeException("加载ZooKeeper集群" + path + "节点配置信息失败！", e);
      }
    }
    Configuration.buildConfig(props);
  }


  public void setZkPaths(List<String> zkPaths) {
    ZKPropertyPlaceholderConfigurer.zkPaths = zkPaths;
  }


  public void setZkClient(CuratorFramework zkClient) {
    ZKPropertyPlaceholderConfigurer.zkClient = zkClient;
  }

  public List<String> getZkPaths() {
    return zkPaths;
  }

  public static CuratorFramework getZkClient() {
    return zkClient;
  }

  public void setWatcher(CuratorWatcher watcher) {
    ZKPropertyPlaceholderConfigurer.watcher = watcher;
  }

}
