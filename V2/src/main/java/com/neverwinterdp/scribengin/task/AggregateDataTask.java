package com.neverwinterdp.scribengin.task;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class AggregateDataTask implements Task{
  private int currentCount;
  private int bufferLimit;
  private int aggregateTupleCount;
  private String name;
  private LinkedList<Tuple> buffer = new LinkedList<Tuple>();
  
  public AggregateDataTask(){
    this(1000);
  }
  
  public AggregateDataTask(int bufferLimit){
    this.currentCount = 0;
    this.aggregateTupleCount = 0;
    this.bufferLimit = bufferLimit;
    this.name = this.getClass().getSimpleName()+"-"+UUID.randomUUID().toString();
  }
  
  @Override
  public Tuple[] execute(Tuple t) {
    buffer.add(t);
    Tuple[] tupleArray = null;
    currentCount++;
    
    
    if(currentCount >= bufferLimit){
      tupleArray = new Tuple[2];
      t.setInvalidData(true);
      tupleArray[0] = t;
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      for(Tuple x: this.buffer){
        try {
          byteStream.write(x.getData());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      Tuple aggregateData = new Tuple("Generated-"+UUID.randomUUID().toString(),
                                      byteStream.toByteArray(),
                                      new CommitLogEntry(this.name,this.aggregateTupleCount, this.aggregateTupleCount));
      aggregateData.setTaskGenerated(true);
      tupleArray[1] = aggregateData;
      this.aggregateTupleCount++;
    }
    else{
      tupleArray = new Tuple[1];
      t.setInvalidData(true);
      tupleArray[0] = t;
    }
    return tupleArray;
  }

  @Override
  public boolean readyToCommit() {
    if(this.currentCount >= this.bufferLimit){
      return true;
    }
    return false;
  }
  
  @Override
  public boolean commit(){
    this.currentCount = 0;
    this.buffer.clear();
    return true;
  }

}
