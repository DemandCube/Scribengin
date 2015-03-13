package com.neverwinterdp.kafka.producer;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * org.apache.kafka.common.errors.TimeoutException
 * 
 * @param <T>
 * */
public class DefaultCallback implements Callback {

  private static AtomicInteger success = new AtomicInteger(0);
  private static AtomicInteger failed = new AtomicInteger(0);

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      failed.getAndIncrement();
      //    System.err.println("callback " + failed.get() + " exception:" + exception);
    }
    else {
      success.getAndIncrement();
      //  System.err.println("callback " + success.get() + " offset:" + metadata.offset());
    }
  }

  public int getSuccessCount() {
    return success.get();
  }

  public int getFailedCount() {
    return failed.get();
  }

  public void resetCounters() {
    success.set(0);
    failed.set(0);
  }
}
