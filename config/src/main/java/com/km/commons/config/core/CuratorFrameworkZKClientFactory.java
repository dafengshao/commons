package com.km.commons.config.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.km.commons.config.api.DefaultZkStartListener;
import com.km.commons.config.api.ZKStartListener;

public class CuratorFrameworkZKClientFactory

{

  private final static Logger logger = LoggerFactory
      .getLogger(CuratorFrameworkZKClientFactory.class);

  private static CuratorFramework zkClient;

  private static List<ZKStartListener> listeners;


  private CuratorFrameworkZKClientFactory() {
    super();
  }


  // 设置Zookeeper启动后需要调用的监听或者，或者需要做的初始化工作。
  public static void setListeners(List<ZKStartListener> listeners) {
    CuratorFrameworkZKClientFactory.listeners = listeners;
  }


  public CuratorFramework getObject() {
    return zkClient;
  }

  public Class<?> getObjectType() {
    return CuratorFramework.class;
  }

  public boolean isSingleton() {
    return true;
  }

  private static CuratorFramework initZkClient() {
    // 1000 是重试间隔时间基数，3 是重试次数
    String zkConnectionString = getSystemPropertiesValue("zk.host");
    String sleepTime = getSystemPropertiesValue("zk.sleepTime", "1000");
    int baseSleepTimeMs = Integer.parseInt(sleepTime);

    String retryTimes = getSystemPropertiesValue("zk.retryTimes", "3");
    int maxRetries = Integer.parseInt(retryTimes);

    String connTimeout = getSystemPropertiesValue("zk.connect.timeout", "10000");
    int connectionTimeoutMs = Integer.parseInt(connTimeout);

    String sessionTimeout = getSystemPropertiesValue("zk.session.timeout", "10000");
    int sessionTimeoutMs = Integer.parseInt(sessionTimeout);

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
    zkClient =
        createWithOptions(zkConnectionString, retryPolicy, connectionTimeoutMs, sessionTimeoutMs);
    registerListeners(zkClient);
    zkClient.start();
    return zkClient;
  }

  public static CuratorFramework getZkClient() {
    if (zkClient == null || zkClient.getState() == CuratorFrameworkState.STOPPED) {
      synchronized (CuratorFrameworkZKClientFactory.class) {
        if (zkClient == null || zkClient.getState() == CuratorFrameworkState.STOPPED) {
          initZkClient();
        }
      }
      return zkClient;
    } else {
      return zkClient;
    }
  }

  private static String getSystemPropertiesValue(String key) {
    // 尝试系统变量中找
    String value = System.getProperty(key);
    if (StringUtils.isBlank(value)) {
      logger.error("无法初始化zk客户端，系统变量未配置{}！", key);
      throw new RuntimeException("无法初始化zk客户端，未配置" + key + "值！");
    }
    Configuration.put(key, value);
    return value;
  }

  private static String getSystemPropertiesValue(String key, String defaultValue) {
    String value = System.getProperty(key);
    if (StringUtils.isBlank(value)) {
      value = defaultValue;
    }
    Configuration.put(key, value);
    return value;
  }

  public void destroy() throws Exception {
    zkClient.close();
  }


  /**
   * 通过自定义参数创建
   */
  public static CuratorFramework createWithOptions(String connectionString,
      RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) {
    return CuratorFrameworkFactory.builder().connectString(connectionString)
        .retryPolicy(retryPolicy).connectionTimeoutMs(connectionTimeoutMs)
        .sessionTimeoutMs(sessionTimeoutMs).build();
  }

  // 注册需要监听的监听者对像.
  private static void registerListeners(CuratorFramework client) {
    client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        logger.info("CuratorFramework state changed: {}", newState);
        if (null == listeners) {
          listeners = new ArrayList<ZKStartListener>();
          listeners.add(new DefaultZkStartListener());
        }
        for (ZKStartListener listener : listeners) {
          listener.executor(client, newState);
        }
      }
    });

    client.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
      @Override
      public void unhandledError(String message, Throwable e) {
        logger.info("CuratorFramework unhandledError: {}", message);
      }
    });
    // client.getCuratorListenable().addListener(new CuratorListener() {
    //
    // @Override
    // public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
    //
    // }
    // }, new Executor() {
    // @Override
    // public void execute(Runnable command) {
    //
    // }
    // });

  }



}
