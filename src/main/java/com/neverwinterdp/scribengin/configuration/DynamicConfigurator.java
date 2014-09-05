package com.neverwinterdp.scribengin.configuration;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.ScribenginContext;
import com.neverwinterdp.scribengin.utils.ConfigurationCommand;
import com.neverwinterdp.scribengin.utils.ScribenginUtils;

// https://github.com/Netflix/archaius/wiki/Users-Guide
/**
 * Take a scenario where we want to stop and instance, have it read a different topic/partition, have it change number of threads
 * 
 * Dynamic configurator is provided a zookeeper location where dynamic configs are kept.
 * 
 * On update we read those configs and dynamically update the app instance.
 * 
 * 
 * */

//e.g start it with a kafka, dynamically config to point correct kafka.
public class DynamicConfigurator {

  private String zkConnectString;
  private String zkPath;
  private ExponentialBackoffRetry retryPolicy =
      new ExponentialBackoffRetry(1000, Integer.MAX_VALUE);
  private ScribenginContext context;

  private static final Logger logger = Logger.getLogger(DynamicConfigurator.class);

  private CuratorFramework curator;
  private NodeCache nodeCache;
  private ConfigurationCommand configurationCommand;


  public DynamicConfigurator(String zkConnectString, String zkPath) {
    super();
    Thread.currentThread().setName("Dynamic-Configurator-Thread");
    this.zkConnectString = zkConnectString;
    this.zkPath = zkPath;
    this.context = new ScribenginContext();
    init();
  }



  private void init() {
    curator = CuratorFrameworkFactory.newClient(zkConnectString, retryPolicy);
    curator.start();
    try {
      curator.blockUntilConnected();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void startWatching() throws Exception {
    nodeCache = new NodeCache(curator, zkPath);
    nodeCache.getListenable().addListener(new Listener());
    nodeCache.start();
  }

  void close() throws IOException {
    nodeCache.close();
  }

  public ScribenginContext getContext() {
    return context;
  }



  public void setContext(ScribenginContext context) {
    this.context = context;
  }

  class Listener implements NodeCacheListener {

    @Override
    public void nodeChanged() throws Exception {
      ChildData currentData = nodeCache.getCurrentData();
      logger.debug("data change watched, and current data = "
          + new String(currentData.getData()));

      configurationCommand = getNewConfigurationCommand(currentData.getData());
      context.put("ConfigCommand", configurationCommand);

    }

    private ConfigurationCommand getNewConfigurationCommand(byte[] data) {
      return ScribenginUtils.toClass(data, ConfigurationCommand.class);
    }
  }
}
