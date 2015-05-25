package com.neverwinter.es.cluster;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ElasticSearchClusterServiceUnitTest {
  static ElasticSearchClusterBuilder clusterBuilder ;
  
  @BeforeClass
  static public void setup() throws Exception {
    clusterBuilder = new ElasticSearchClusterBuilder() ;
  }

  @AfterClass
  static public void teardown() throws Exception {
    clusterBuilder.destroy();
  }
  
  @Test
  public void test() throws Exception {
    clusterBuilder.install();
    for(int i = 0; i < 10; i++) {
      Thread.sleep(1000);
      clusterBuilder.esServer[0].getLogger().info("wait: this is a test");
    }
    clusterBuilder.uninstall();
  }
}
