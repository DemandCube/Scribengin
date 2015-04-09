package com.neverwinterdp.tool.message;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

public class MessageTrackerUnitTest {

  @Test
  public void testPartitionGeneratedMessageTracker() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 0, 10);
    log(tracker, 10, 20);
    log(tracker, 21, 30);
    log(tracker, 40, 50);
    log(tracker, 30, 40);
    log(tracker, 30, 60);
    log(tracker, 100, 200);
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testOutOfOrderPartitionGeneratedMessageTracker() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 100, 0);
    log(tracker, 200, 80);
    log(tracker, 1000, 200);
    tracker.dump(System.out, "Sequence Number Tracker");
    tracker.optimize();
    tracker.dump(System.out, "Sequence Number Tracker");
  }  
  
  @Test
  public void testInSequence() throws Exception {
    MessageTracker messageTracker = new MessageTracker();
    for (int i = 0; i < 100; i++) {
      messageTracker.log(0, i);
    }
    for (int i = 100; i < 1000; i++) {
      messageTracker.log(0, i);
    }
    messageTracker.dump(System.out);
    assertTrue(messageTracker.isInSequence());
  }

  @Test
  public void testJunitReport() throws Exception {
    MessageTracker messageTracker = new MessageTracker();
    for (int i = 0; i < 100; i++) {
      messageTracker.log(0, i);
    }
    
    for (int i = 0; i < 100; i++) {
      messageTracker.log(1, i);
    }

    messageTracker.optimize();
    messageTracker.junitReport("build/messageTracker.xml");
  }
  
  @Test
  public void testJunitReportNotInSequence() throws Exception {
    MessageTracker messageTracker = new MessageTracker();
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
      messageTracker.log(0, random.nextInt(100));
    }

    for (int i = 0; i < 100; i++) {
      messageTracker.log(1, i);
    }

    messageTracker.dump(System.out);
    messageTracker.optimize();
    messageTracker.junitReport("build/messageTrackerNotInSequence.xml");
  }
  
  //partition 0 contains all 0, partition 1 contains all 1
  @Test
  public void testJunitReportAllDuplicates() throws Exception {
    MessageTracker messageTracker = new MessageTracker();
    for (int i = 0; i < 1000; i++) {
      messageTracker.log(0, 0);
    }

    for (int i = 0; i < 1000; i++) {
      messageTracker.log(1, i%10);
    }

    messageTracker.optimize();
    messageTracker.junitReport("build/messageTrackerAllDuplicates.xml");
    messageTracker.dump(System.out);
  }

  private void log(PartitionMessageTracker tracker, int from, int to) {
    if (from < to) {
      for (int num = from; num <= to; num++) {
        tracker.log(num);
      }
    } else {
      for (int num = from; num >= to; num--) {
        tracker.log(num);
      }
    }
  }
}
