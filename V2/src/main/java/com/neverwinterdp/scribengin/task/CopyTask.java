package com.neverwinterdp.scribengin.task;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class CopyTask implements Task{
  private int count;
  private int bufferSize;
  
  public CopyTask(){
    this(5000);
  }
  
  public CopyTask(int bufferSize){
    this.count = 0;
    this.bufferSize = bufferSize;
  }
  
  @Override
  public Tuple[] execute(Tuple t) {
    Tuple[] tupleArray = new Tuple[1];
    tupleArray[0] = t;
    this.count++;
    return tupleArray;
  }

  @Override
  public boolean readyToCommit() {
    if(count >= this.bufferSize){
      return true;
    }
    return false;
  }
  
  @Override
  public boolean commit(){
    count = 0;
    return true;
  }

}
