package com.neverwinterdp.scribengin.tuple.counter;

import java.util.HashMap;

public class InMemoryTupleCounter implements TupleCounter{
  static public enum tupleStates {VALID, INVALID, CREATED, WRITTEN};
  HashMap<tupleStates, Long> counterMap;
  HashMap<tupleStates, Long> buffer;
  
  public InMemoryTupleCounter(){
    counterMap = new HashMap<tupleStates,Long>();
    buffer = new HashMap<tupleStates,Long>();
    for(tupleStates i: tupleStates.values()){
      counterMap.put(i, (long) 0);
      buffer.put(i, (long) 0);
    }
  }
  
  @Override
  public void incrementValid() {
    buffer.put(tupleStates.VALID, buffer.get(tupleStates.VALID) + 1);
  }

  @Override
  public void incrementWritten() {
    buffer.put(tupleStates.WRITTEN, buffer.get(tupleStates.WRITTEN) + 1);
  }

  //@Override
  //public void incrementProcessed() {
  //  counterMap.put(tupleStates.PROCESSED, counterMap.get(tupleStates.PROCESSED) + 1);
  //}

  @Override
  public void incrementInvalid() {
    buffer.put(tupleStates.INVALID, buffer.get(tupleStates.INVALID) + 1);
  }

  @Override
  public void incrementCreated() {
    buffer.put(tupleStates.CREATED, buffer.get(tupleStates.CREATED) + 1);
  }

  @Override
  public void addValid(long toAdd) {
    buffer.put(tupleStates.VALID, buffer.get(tupleStates.VALID) + toAdd);
  }

  @Override
  public void addWritten(long toAdd) {
    buffer.put(tupleStates.WRITTEN, buffer.get(tupleStates.WRITTEN) + toAdd);
  }

  //@Override
  //public void addProcessed(int toAdd) {
  //  counterMap.put(tupleStates.PROCESSED, counterMap.get(tupleStates.PROCESSED) + toAdd);
  //}

  @Override
  public void addInvalid(long toAdd) {
    buffer.put(tupleStates.INVALID, buffer.get(tupleStates.INVALID) + toAdd);
  }

  @Override
  public void addCreated(long toAdd) {
    buffer.put(tupleStates.CREATED, buffer.get(tupleStates.CREATED) + toAdd);
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
    System.out.println(this.getValid() + " "+ this.getCreated() +" "+ this.getInvalid()+" "+this.getWritten());
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

  @Override
  public void commit() {
    for(tupleStates i: tupleStates.values()){
      counterMap.put(i, counterMap.get(i) + buffer.get(i));
    }
    this.clearBuffer();
  }

  @Override
  public void clearBuffer(){
    for(tupleStates i: tupleStates.values()){
      buffer.put(i, 0L);
    }
  }
}
