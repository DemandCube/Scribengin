package com.neverwinterdp.scribengin.task;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class DumbTask implements Task{
  private int count;
  public DumbTask(){
    count = 0;
  }

  @Override
  public Tuple execute(Tuple t) {
    if(++count == 10){
      count = 0;
      return null;
    }
    return t;
  }

}
