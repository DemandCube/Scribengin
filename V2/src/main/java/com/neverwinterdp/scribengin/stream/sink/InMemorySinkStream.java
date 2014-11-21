package com.neverwinterdp.scribengin.stream.sink;

import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class InMemorySinkStream implements SinkStream{
  private LinkedList<Tuple> list;
  private LinkedList<Tuple> buffer;
  
  //private SinkPartitioner sp;
  private String name;
  
  public InMemorySinkStream(SinkPartitioner sp){
    this.list = new LinkedList<Tuple>();
    this.buffer = new LinkedList<Tuple>();
    //this.sp = sp;
    this.name = this.getClass().getSimpleName() +"-"+UUID.randomUUID().toString();
  }
  
  public InMemorySinkStream(){
    this(null);
  }
  
  @Override
  public void setSinkPartitioner(SinkPartitioner sp) {
    //this.sp = sp;
  }

  public LinkedList<Tuple> getData(){
    return this.list;
  }

  @Override
  public String getName() {
    return this.name;
  }

/*
  @Override
  public Tuple[] getCommittedData() {
    return Arrays.copyOf( this.list.toArray(),  this.list.toArray().length, Tuple[].class);
  }

  @Override
  public Tuple readFromOffset(long startOffset, long endOffset){
    for(Tuple t: list){
      if(t.getCommitLogEntry().getStartOffset() == startOffset &&
          t.getCommitLogEntry().getEndOffset() == endOffset){
        return t;
      }
    }
    return null;
  }
  
  @Override
  public boolean removeFromOffset(long startOffset, long endOffset){
    for(int i=0; i< list.size(); i++){
      if(list.get(i).getCommitLogEntry().getStartOffset() == startOffset &&
          list.get(i).getCommitLogEntry().getEndOffset() == endOffset){
        list.remove(i);
        return true;
      }
    }
    return false;
  }

*/

  @Override
  public boolean prepareCommit() {
    return true;
  }


  @Override
  public boolean commit() {
    if(buffer.isEmpty()){
      return true;
    }
    return list.addAll(buffer);
  }


  @Override
  public boolean clearCommit() {
    buffer.clear();
    return true;
  }


  @Override
  public boolean updateOffSet() {
    buffer.clear();
    return true;
  }


  @Override
  public long getBufferSize() {
    return buffer.size();
  }


  @Override
  public boolean append(Tuple t) {
    return buffer.add(t);
  }


  @Override
  public boolean rollBack() {
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
  
}
