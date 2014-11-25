package com.neverwinterdp.scribengin.tuple.counter;

import java.util.HashMap;

public class InMemoryTupleCounter implements TupleCounter{
  static public enum tupleStates {VALID, INVALID, CREATED, WRITTEN};
  HashMap<tupleStates, Long> counterMap;
  
  public InMemoryTupleCounter(){
    counterMap = new HashMap<tupleStates,Long>();
    for(tupleStates i: tupleStates.values()){
      counterMap.put(i, (long) 0);
    }
  }
  
  @Override
  public void incrementValid() {
    counterMap.put(tupleStates.VALID, counterMap.get(tupleStates.VALID) + 1);
  }

  @Override
  public void incrementWritten() {
    counterMap.put(tupleStates.WRITTEN, counterMap.get(tupleStates.WRITTEN) + 1);
  }

  //@Override
  //public void incrementProcessed() {
  //  counterMap.put(tupleStates.PROCESSED, counterMap.get(tupleStates.PROCESSED) + 1);
  //}

  @Override
  public void incrementInvalid() {
    counterMap.put(tupleStates.INVALID, counterMap.get(tupleStates.INVALID) + 1);
  }

  @Override
  public void incrementCreated() {
    counterMap.put(tupleStates.CREATED, counterMap.get(tupleStates.CREATED) + 1);
  }

  @Override
  public void addValid(long toAdd) {
    counterMap.put(tupleStates.VALID, counterMap.get(tupleStates.VALID) + toAdd);
  }

  @Override
  public void addWritten(long toAdd) {
    counterMap.put(tupleStates.WRITTEN, counterMap.get(tupleStates.WRITTEN) + toAdd);
  }

  //@Override
  //public void addProcessed(int toAdd) {
  //  counterMap.put(tupleStates.PROCESSED, counterMap.get(tupleStates.PROCESSED) + toAdd);
  //}

  @Override
  public void addInvalid(long toAdd) {
    counterMap.put(tupleStates.INVALID, counterMap.get(tupleStates.INVALID) + toAdd);
  }

  @Override
  public void addCreated(long toAdd) {
    counterMap.put(tupleStates.CREATED, counterMap.get(tupleStates.CREATED) + toAdd);
  }
  
  @Override
  public long getValid() {
    return counterMap.get(tupleStates.VALID);
  }

  @Override
  public long getWritten() {
    return counterMap.get(tupleStates.WRITTEN);
  }

  //@Override
  //public long getProcessed() {
  //  return counterMap.get(tupleStates.PROCESSED);
  //  }

  @Override
  public long getInvalid() {
    return counterMap.get(tupleStates.INVALID);
  }

  @Override
  public long getCreated() {
    return counterMap.get(tupleStates.CREATED);
  }

  //READ + CREATED == INVALID + PROCESSED + CREATED == WRITTEN) should always be true
  //READ + CREATED + INVALID + PROCESSED + CREATED == WRITTEN) should always be true
  @Override
  public boolean validateCounts() {
    return (this.getValid() + this.getCreated() + this.getInvalid()) == this.getWritten();
  }

  @Override
  public String toString(){
    StringBuilder result = new StringBuilder();
    String NEW_LINE = System.getProperty("line.separator");

    result.append("VALID   : ");
    result.append(Long.toString(this.getValid()));
    result.append(NEW_LINE);
    
    result.append("INVALID : ");
    result.append(Long.toString(this.getInvalid()));
    result.append(NEW_LINE);
    
    result.append("CREATED : ");
    result.append(Long.toString(this.getCreated()));
    result.append(NEW_LINE);
    
    result.append("WRITTEN : ");
    result.append(Long.toString(this.getWritten()));
    result.append(NEW_LINE);
    
    return result.toString();
  }

}
