package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.neverwinterdp.scribengin.fixture.KafkaFixture;
import com.neverwinterdp.scribengin.fixture.ZookeeperFixture;


public class ScribeKafkaTest {
  private static ZookeeperFixture zookeeper;
  private static KafkaFixture kf1;
  private static KafkaFixture kf2;

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

    System.out.println("about to start kafka");//xxx
    kf1 = new KafkaFixture("0.8.1", "127.0.0.1", 19876,
      zookeeper.getHost(),
      zookeeper.getPort());
    kf2 = new KafkaFixture("0.8.1", "127.0.0.1", 19877,
      zookeeper.getHost(),
      zookeeper.getPort());

    kf1.start();
    kf2.start();
  }


  @AfterClass
  public static void teardown() throws IOException {
    System.out.println("calling teardown.."); //xxx
    kf1.stop();
    kf2.stop();
    zookeeper.stop();
  }


  //@Test
  //public void testKafka() {
  //}
}
