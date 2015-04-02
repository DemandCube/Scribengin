package com.neverwinterdp.tool.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class MessageTrackerUnitTest {

  @Test
  public void testCase1() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 0, 10);
    log(tracker, 11, 20);
    log(tracker, 21, 30);
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 30);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 0);
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase2() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 0, 10);
    log(tracker, 10, 20);
    log(tracker, 20, 30);
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 30);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 2);
    assertTrue(tracker.getSequenceMap().get(0).getDuplicatedNumbers().containsAll(Arrays.asList(10, 20)));
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase3() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 0, 10);
    log(tracker, 5, 20);
    log(tracker, 15, 30);
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 30);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 12);
    assertTrue(tracker.getSequenceMap().get(0).getDuplicatedNumbers()
        .containsAll(Arrays.asList(5, 6, 7, 8, 9, 10, 15, 16, 17, 18, 19, 20)));
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase4() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 0, 10);
    log(tracker, 2, 8);
    log(tracker, 4, 6);
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 10);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 10);
    assertTrue(tracker.getSequenceMap().get(0).getDuplicatedNumbers().containsAll(Arrays.asList(2, 3, 4, 5, 6, 7, 8)));
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase5() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 0, 10);
    log(tracker, 3, 5);
    log(tracker, 7, 9);
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 10);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 6);
    assertTrue(tracker.getSequenceMap().get(0).getDuplicatedNumbers().containsAll(Arrays.asList(3, 4, 5, 7, 8, 9)));
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase6() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 0, 10);
    log(tracker, 15, 20);
    log(tracker, 25, 30);
    assertEquals(tracker.getSequenceMap().size(), 3);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 10);
    assertEquals(tracker.getSequenceMap().get(1).getFrom(), 15);
    assertEquals(tracker.getSequenceMap().get(1).getCurrent(), 20);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent() + 5, tracker.getSequenceMap().get(1).getFrom());
    assertEquals(tracker.getSequenceMap().get(1).getCurrent() + 5, tracker.getSequenceMap().get(2).getFrom());
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  public void testCase7() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 0, 10);
    log(tracker, 15, 20);
    log(tracker, 18, 30);
    assertEquals(tracker.getSequenceMap().size(), 3);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 10);
    assertEquals(tracker.getSequenceMap().get(1).getFrom(), 15);
    assertEquals(tracker.getSequenceMap().get(1).getCurrent(), 20);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 4);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent() + 5, tracker.getSequenceMap().get(1).getFrom());
    assertEquals(tracker.getSequenceMap().get(1).getCurrent() + 5, tracker.getSequenceMap().get(2).getFrom());
    assertTrue(tracker.getSequenceMap().get(0).getDuplicatedNumbers().containsAll(Arrays.asList(18, 19, 20)));
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase8() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 10, 0);
    log(tracker, 20, 11);
    log(tracker, 30, 21);
    tracker.optimize();
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 30);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 0);
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase9() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 10, 0);
    log(tracker, 20, 10);
    log(tracker, 30, 20);
    tracker.optimize();
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 30);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 2);
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase10() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 10, 0);
    log(tracker, 20, 5);
    log(tracker, 30, 15);
    tracker.optimize();
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 30);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 12);
    assertTrue(tracker.getSequenceMap().get(0).getDuplicatedNumbers()
        .containsAll(Arrays.asList(5, 6, 7, 8, 9, 10, 15, 16, 17, 18, 19, 20)));
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase11() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 10, 0);
    log(tracker, 8, 2);
    log(tracker, 6, 4);
    tracker.optimize();
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 10);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 10);
    assertTrue(tracker.getSequenceMap().get(0).getDuplicatedNumbers().containsAll(Arrays.asList(2, 3, 4, 5, 6, 7, 8)));
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase12() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 10, 0);
    log(tracker, 5, 3);
    log(tracker, 9, 7);
    tracker.optimize();
    assertEquals(tracker.getSequenceMap().size(), 1);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 10);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 6);
    assertTrue(tracker.getSequenceMap().get(0).getDuplicatedNumbers().containsAll(Arrays.asList(3, 4, 5, 7, 8, 9)));
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase13() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 10, 0);
    log(tracker, 20, 15);
    log(tracker, 30, 25);
    tracker.optimize();
    assertEquals(tracker.getSequenceMap().size(), 3);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 10);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 0);
    tracker.dump(System.out, "Sequence Number Tracker");
  }

  @Test
  public void testCase14() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0);
    log(tracker, 10, 0);
    log(tracker, 20, 15);
    log(tracker, 30, 18);
    tracker.optimize();
    assertEquals(tracker.getSequenceMap().size(), 2);
    assertEquals(tracker.getSequenceMap().get(0).getFrom(), 0);
    assertEquals(tracker.getSequenceMap().get(0).getCurrent(), 10);
    assertEquals(tracker.getSequenceMap().get(0).getDuplicatedCount(), 0);
    assertEquals(tracker.getSequenceMap().get(1).getDuplicatedCount(), 3);
    assertTrue(tracker.getSequenceMap().get(1).getDuplicatedNumbers().containsAll(Arrays.asList(18, 19, 20)));
    tracker.dump(System.out, "Sequence Number Tracker");
  }

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
    messageTracker.junitReport("build/messageTracker.xml", false);
  }
  
  //partition 0 (probably) not in sequence, partition 1 in sequence
  @Test
  public void testJunitReportNotInSequence() throws Exception {
    MessageTracker messageTracker = new MessageTracker();
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      messageTracker.log(0, random.nextInt(2000));
    }

    for (int i = 0; i < 1000; i++) {
      messageTracker.log(1, i);
    }

    messageTracker.optimize();
    messageTracker.junitReport("build/messageTrackerNotInSequence.xml", false);
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
    messageTracker.junitReport("build/messageTrackerAllDuplicates.xml", false);
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
