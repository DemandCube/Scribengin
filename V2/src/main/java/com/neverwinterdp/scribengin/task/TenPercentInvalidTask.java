package com.neverwinterdp.scribengin.task;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class TenPercentInvalidTask implements Task{
  private int count;
  
  public TenPercentInvalidTask(){
    count = 0;
  }
  
  @Override
  public Tuple[] execute(Tuple t) {
    if(++count % 10 == 0){
      t.setInvalidData(true);
    }
    Tuple[] tupleArray = new Tuple[1];
    tupleArray[0] = t;
    return tupleArray;
  }

  @Override
  public boolean readyToCommit() {
    if(count > 9){
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
