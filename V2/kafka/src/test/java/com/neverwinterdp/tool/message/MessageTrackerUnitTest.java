package com.neverwinterdp.tool.message;

import org.junit.Test;

import com.neverwinterdp.tool.message.PartitionMessageTracker;

public class MessageTrackerUnitTest {
  //TODO: figure out different cases, add unit test and assert
  //You can add more method to PartitionMessageTracker and MessageTracker to help verify
  
  @Test
  public void testPartitionGeneratedMessageTracker() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0) ;
    log(tracker, 0, 10)  ;
    log(tracker, 10, 20) ;
    log(tracker, 21, 30) ;
    log(tracker, 40, 50) ;
    log(tracker, 30, 40) ;
    log(tracker, 30, 60) ;
    log(tracker, 100, 200) ;
    tracker.dump(System.out, "Sequence Number Tracker");
  }
  
  @Test
  public void testOutOfOrderPartitionGeneratedMessageTracker() throws Exception {
    PartitionMessageTracker tracker = new PartitionMessageTracker(0) ;
    log(tracker, 100, 0);
    log(tracker, 200, 80);
    log(tracker, 1000, 200);
    tracker.optimize();
    tracker.dump(System.out, "Sequence Number Tracker");
  }
  
  
  private void log(PartitionMessageTracker tracker, int from, int to) {
    if(from < to) {
      for(int num = from; num <= to; num++) {
        tracker.log(num);
      }
    } else {
      for(int num = from; num >= to; num--) {
        tracker.log(num);
      } 
    }
  }
}
