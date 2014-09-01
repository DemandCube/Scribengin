package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class ScribeKafkaTest {
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
  }

  @AfterClass
  public static void teardown() {

  }

  @Test
  public void testKafka() {
  }
}
