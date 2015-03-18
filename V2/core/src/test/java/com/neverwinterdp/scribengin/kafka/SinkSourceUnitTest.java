package com.neverwinterdp.scribengin.kafka;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.kafka.sink.KafkaSink;
import com.neverwinterdp.scribengin.kafka.source.KafkaSource;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;
import com.neverwinterdp.server.kafka.KafkaCluster;

public class SinkSourceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/cluster", 1, 1);
    cluster.setNumOfPartition(5);
    cluster.start();
    Thread.sleep(3000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testKafkaSource() throws Exception {
    String zkConnect = cluster.getZKConnect();
    System.out.println("zkConnect = " + zkConnect);
    String TOPIC = "hello.topic" ;
    KafkaSink sink = new KafkaSink("hello", zkConnect, TOPIC) ;
    SinkStream stream = sink.newStream();
    SinkStreamWriter writer = stream.getWriter();
    for(int i = 0; i < 10; i++) {
      String hello = "Hello " + i ;
      Record record = new Record("key-" + i, hello.getBytes());
      writer.append(record);
    }
    writer.close();
    
    KafkaSource source = new KafkaSource("hello", cluster.getZKConnect(), TOPIC);
    SourceStream[] streams = source.getStreams();
    Assert.assertEquals(5, streams.length);
    for(int i = 0; i < streams.length; i++) {
      System.out.println("Stream id: " + streams[i].getDescriptor().getId());
      SourceStreamReader reader = streams[i].getReader("kafka");
      Record record = null;
      while((record = reader.next()) != null) {
        System.out.println("Record: " + new String(record.getData()));
      }
    }
  }
}
