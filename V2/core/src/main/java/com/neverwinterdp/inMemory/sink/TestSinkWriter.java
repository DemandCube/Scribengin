package com.neverwinterdp.inMemory.sink;


import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class TestSinkWriter implements SinkStreamWriter{
  private LinkedList<Record> list;
  private LinkedList<Record> buffer;
  int numMessagesWritten;
  
  private String name;
  
  public TestSinkWriter(){
    this.list = new LinkedList<Record>();
    this.buffer = new LinkedList<Record>();
    this.numMessagesWritten = 0;
    //this.sp = sp;
    this.name = this.getClass().getSimpleName() +"-"+UUID.randomUUID().toString();
  }
  
  

  public LinkedList<Record> getData(){
    return this.list;
  }

  public String getName() {
    return this.name;
  }


  @Override
  public boolean prepareCommit() {
    return true;
  }


  @Override
  public boolean commit() {
    if(buffer.isEmpty()){
      return true;
    }
    this.numMessagesWritten += list.size();
    //return list.addAll(buffer);
    return list.addAll(buffer);
  }



  @Override
  public void completeCommit() {
    buffer.clear();
    //return true;
  }

  public long getBufferSize() {
    return buffer.size();
  }




  @Override
  public boolean rollback() throws Exception{
    for(int i = 0; i < buffer.size(); i++){
      for(int j = 0; j < list.size(); j++){
        if(buffer.get(i).equals(list.get(j))){
          list.remove(j);
        }
      }
    }
    buffer.clear();
    return true;
  }
  

  @Override
  public void close() throws Exception {
    
  }


  @Override
  public void append(Record record) throws Exception {
    buffer.add(record);
  }


  
  public int getNumMessagesWritten(){
    return this.numMessagesWritten;
  }



  @Override
  public void clearBuffer() {
    buffer.clear();
  }
}