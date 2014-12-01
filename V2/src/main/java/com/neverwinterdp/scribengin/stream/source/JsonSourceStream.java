package com.neverwinterdp.scribengin.stream.source;

import java.util.LinkedList;
import java.util.Random;
import java.util.UUID;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.streamdescriptor.JsonSourceStreamDescriptor;
import com.neverwinterdp.scribengin.stream.streamdescriptor.StreamDescriptor;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class JsonSourceStream implements SourceStream {

  private String name;
  private String key;
  private LinkedList<Tuple> data;
  private int currentOffset;
  private int lastCommitted;
  
  public JsonSourceStream(){
    this("data");
  }
  
  public JsonSourceStream(String k){
    key = k;
    name = this.getClass().getSimpleName() +"-"+UUID.randomUUID().toString();
    currentOffset = 0;
    lastCommitted = 0;
    
    data = new LinkedList<Tuple>();
  }
  
  public JsonSourceStream(JsonSourceStreamDescriptor sDesc){
    this.name = sDesc.getName();
    this.currentOffset = sDesc.getCurrentOffset();
    this.lastCommitted = sDesc.getLastCommittedOffset();
    this.key = sDesc.getKey();
    
    data = new LinkedList<Tuple>();
  }
  
  public LinkedList<Tuple> getData(){
    return data;
  }
  
  @Override
  public Tuple readNext() {
    if(currentOffset < data.size()){
      return data.get(currentOffset++);
    }
    Random rand = new Random();
    String jsonString = "{\""+this.key+"\":"+ 
        "[{\"field1\": \""+Integer.toString(rand.nextInt(10000))+"\", \"field2\": \""+Integer.toString(rand.nextInt(10000))+"\", \"field3\": \""+Integer.toString(rand.nextInt(10000))+"\"}," + 
        "{\"field1\": \""+Integer.toString(rand.nextInt(10000))+"\", \"field2\": \""+Integer.toString(rand.nextInt(10000))+"\", \"field3\": \""+Integer.toString(rand.nextInt(10000))+"\"}," +
        "{\"field1\": \""+Integer.toString(rand.nextInt(10000))+"\", \"field2\": \""+Integer.toString(rand.nextInt(10000))+"\", \"field3\": \""+Integer.toString(rand.nextInt(10000))+"\"}]}";
    
    Tuple t = new Tuple(Integer.toString(currentOffset), 
                      jsonString.getBytes(),
                      new CommitLogEntry(this.getName(), currentOffset, currentOffset));
    data.add(t);
    
    currentOffset++;
    return t;
  }
  
  /*
  @Override
  public Tuple readFromOffset(long startOffset, long endOffset) {
    if(startOffset < data.size()){
      return data.get((int)(startOffset));
    }
    else{
      return null;
    }
  }
   */

  @Override
  public boolean hasNext() {
    return true;
  }


  @Override
  public String getName() {
    return this.name;
  }


  @Override
  public boolean prepareCommit() {
    return true;
  }


  @Override
  public boolean commit() {
    this.lastCommitted = this.currentOffset; 
    return true;
  }


  @Override
  public boolean clearBuffer() {
    this.currentOffset = this.lastCommitted;
    return false;
  }


  @Override
  public boolean completeCommit() {
    this.lastCommitted = this.currentOffset; 
    return true;
  }

  @Override
  public boolean rollBack() {
    return true;
  }

  @Override
  public StreamDescriptor getStreamDescriptor() {
    return new JsonSourceStreamDescriptor(this.getName(), this.lastCommitted, this.currentOffset,this.key);
  }


}
