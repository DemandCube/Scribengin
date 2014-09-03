package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class ScribeKafkaTest {
  private static ZookeeperFixture zookeeper;
  private static KafkaFixture kf;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    System.out.println("calling setup..."); //xxx
    Process p = Runtime.getRuntime().exec("script/bootstrap_kafka.sh servers");
    p.waitFor();
    BufferedReader reader =
      new BufferedReader(new InputStreamReader(p.getInputStream()));

    String line = "";
    while ((line = reader.readLine())!= null) {
      System.out.println(line);//xxx
    }

    zookeeper = new ZookeeperFixture("0.8.1", "127.0.0.1", 2323);
    zookeeper.start();

    kf = new KafkaFixture("0.8.1", "127.0.0.1", 9876,
      zookeeper.getHost(),
      zookeeper.getPort());
    kf.start();
  }


  @AfterClass
  public static void teardown() throws IOException {
    System.out.println("calling teardown.."); //xxx
    zookeeper.stop();
  }


  @Test
  public void testKafka() {
  }
}
