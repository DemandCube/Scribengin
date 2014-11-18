package com.neverwinterdp.scribengin.master;

import org.junit.Assert;
import org.junit.Test;

import com.beust.jcommander.JCommander;

public class MasterConfigUnitTest {
  
  @Test
  public void testMasterConfig() throws Exception {
    String[] args = {
      "--registry-factory", "ZookeeperFactory", "--registry-connect", "127.0.0.1:2181"
    } ;
    MasterConfig config = new MasterConfig();
    new JCommander(config, args);
    Assert.assertEquals("ZookeeperFactory", config.getRegistryConfig().getFactory());
    Assert.assertEquals("127.0.0.1:2181", config.getRegistryConfig().getConnect());
  }
}
