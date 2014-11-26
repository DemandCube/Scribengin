package com.neverwinterdp.scribengin.task;

import java.util.UUID;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class DoubleTheTuplesTask implements Task{
  private int tupleCount;
  
  public DoubleTheTuplesTask(){
    this.tupleCount = 0;
  }
  
  @Override
  public Tuple[] execute(Tuple t) {
    Tuple[] tupleArray = new Tuple[2];
    Tuple t2 = new Tuple("Generated-"+UUID.randomUUID().toString(), t.getData(), t.getCommitLogEntry());
    t2.setTaskGenerated(true);
    
    
    tupleArray[0] = t;
    tupleArray[1] = t2;
    
    this.tupleCount += 2;
    return tupleArray;
  }

  @Override
  public boolean readyToCommit() {
    if(this.tupleCount > 9){
      return true;
    }
    
    return false;
  }
  
  @Override
  public boolean commit(){
    this.tupleCount = 0;
    return true;
  }
}
