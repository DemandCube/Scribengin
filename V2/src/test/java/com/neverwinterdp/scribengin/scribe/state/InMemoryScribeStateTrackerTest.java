package com.neverwinterdp.scribengin.scribe.state;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class InMemoryScribeStateTrackerTest {
  @Test
  public void testInMemoryScribeState(){
    ScribeStateTracker sst = new InMemoryScribeStateTracker();
    assertEquals(ScribeState.UNINITIALIZED, sst.getScribeState());
    for(ScribeState state: ScribeState.values()){
      sst.setState(state);
      assertEquals(state, sst.getScribeState());
    }
  }
}
