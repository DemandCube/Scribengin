package com.neverwinter.es.tool.server;

import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.tool.server.ElasticSearchServer;
import com.neverwinterdp.es.tool.server.EmbededElasticSearchServerSet;
import com.neverwinterdp.util.FileUtil;

public class ServerSetUnitTest {
  
  EmbededElasticSearchServerSet serverSet ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/elasticsearch", false);
    serverSet = new EmbededElasticSearchServerSet("build/elasticsearch", 9300, 2, new HashMap<String, String>()) ;
    serverSet.start();
    Thread.sleep(12000); //wait to make sure that the server are launched
  }
  
  @After
  public void teardown() throws Exception {
    serverSet.shutdown();
  }
  
  @Test
  public void test() throws Exception {
    ESClient esclient = new ESClient(new String[] { "127.0.0.1:9300", "127.0.0.1:9301" });
    ElasticSearchServer server1 = serverSet.getServer("elasticsearch-1");
    Logger server1Logger = server1.getLogger();
    for(int i = 0; i < 10; i++) {
      Thread.sleep(1000);
      server1Logger.info("wait: this is a test");
    }
  }
}
