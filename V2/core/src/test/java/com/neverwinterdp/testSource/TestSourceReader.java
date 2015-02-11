package com.neverwinterdp.testSource;

import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.CommitPoint;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class TestSourceReader implements SourceStreamReader{

  int lastCommitted;
  int currNum;
  String name;
  
  public TestSourceReader(int startNum){
    this.currNum = startNum;
    this.lastCommitted = startNum;
    this.name = this.getClass().getSimpleName() +"-"+UUID.randomUUID().toString();
  }
  
  
  public TestSourceReader(){
    this(0);
  }
  

  @Override
  public boolean prepareCommit() {
    return true;
  }

  @Override
  public CommitPoint commit() {
    return new CommitPoint(this.name, this.lastCommitted, this.currNum);
    //return true;
  }

  @Override
  public void clearBuffer() {
    this.currNum = this.lastCommitted;
  }

  @Override
  public void completeCommit() {
    this.lastCommitted = this.currNum;
    //return true;
  }

  @Override
  public void rollback() {
    this.currNum = this.lastCommitted;
    //return true;
  }

  @Override
  public Record next() {
    this.currNum++;
    Record r = new Record(Integer.toString(this.currNum), 
                          Integer.toString(this.currNum).getBytes());
    return r;
  }

  //@Override
  //public boolean hasNext() {
  //  return this.currNum < Integer.MAX_VALUE;
  //}

  @Override
  public String getName() {
    return this.name;
  }


  @Override
  public Record[] next(int size) throws Exception {
    LinkedList<Record> x = new LinkedList<Record>();
    for(int i = 0; i< size; i++){
      x.add(this.next());
    }
    return (Record[]) x.toArray();
  }


  @Override
  public void close() throws Exception {
    
  }

  
  public int getNumMessagesWritten(){
    return this.lastCommitted;
  }



}
