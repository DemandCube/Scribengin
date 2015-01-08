package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.sink.S3Module;
import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.sink.S3SinkStream;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.stream.source.SequentialIntSourceStream;
import com.neverwinterdp.scribengin.task.CopyTask;

public class ScribeS3Test {


  @Test
  public void testScribe() throws Exception {
    int bufferLimit = 30;

    Injector injector = Guice.createInjector(new S3Module("s3.default.properties","topicTest",1,true));
    SinkStream sink = injector.getInstance(S3SinkStream.class);
    Scribe scribe = new ScribeImpl(new SequentialIntSourceStream(), sink, new InMemorySinkStream(new DumbSinkPartitioner()), new CopyTask(bufferLimit));
    assertTrue(scribe.init());
    assertTrue(scribe.getTupleTracker().validateCounts());
    System.err.println("start");
    scribe.start();
    Thread.sleep(1000);
    System.err.println("stop");
    scribe.stop();
    Thread.sleep(1000);
    assertTrue(scribe.getTupleTracker().getWritten() > 0);


  }
}
