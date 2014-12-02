package com.neverwinterdp.scribengin.scribe.state;

public class InMemoryScribeStateTracker implements ScribeStateTracker {
  ScribeState state;
  
  public InMemoryScribeStateTracker(){
    state = ScribeState.UNINITIALIZED;
  }
  
  public InMemoryScribeStateTracker(ScribeState s){
    state = s;
  }
  
  @Override
  public ScribeState getScribeState() {
    return state;
  }

  @Override
  public void setState(ScribeState s) {
    state = s;
  }

}
