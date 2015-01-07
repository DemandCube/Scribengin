package com.neverwinterdp.scribengin.kafka;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dependency.KafkaCluster;
import com.neverwinterdp.scribengin.kafka.sink.SinkImpl;
import com.neverwinterdp.scribengin.kafka.source.SourceImpl;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class SinkSourceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/cluster", 1, 1);
    cluster.setNumOfPartition(5);
    cluster.start();
    Thread.sleep(2000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testKafkaSource() throws Exception {
    SinkImpl sink = new SinkImpl("hello", cluster.getZKConnect(), "hello") ;
    SinkStream stream = sink.newStream();
    SinkStreamWriter writer = stream.getWriter();
    for(int i = 0; i < 10; i++) {
      String hello = "Hello " + i ;
      Record record = new Record("key-" + i, hello.getBytes());
      writer.append(record);
    }
    writer.close();
    
    SourceImpl source = new SourceImpl("hello", cluster.getZKConnect(), "hello");
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
