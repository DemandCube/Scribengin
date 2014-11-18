package com.neverwinterdp.scribengin.sink;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class InMemorySinkStream implements SinkStream{
  private LinkedList<Tuple> list;
  private SinkPartitioner sp;
  private String name;
  
  public InMemorySinkStream(SinkPartitioner sp){
    this.list = new LinkedList<Tuple>();
    this.sp = sp;
    this.name = UUID.randomUUID().toString();
  }
  
  
  @Override
  public boolean writeTuple(Tuple t) {
    sp.getPartition();
    return list.add(t);
  }

  @Override
  public boolean openStream() {
    list.clear();
    return true;
  }

  @Override
  public boolean closeStream() {
    list.clear();
    return true;
  }
  
  @Override
  public void setSinkPartitioner(SinkPartitioner sp) {
    this.sp = sp;
  }

  public LinkedList<Tuple> getData(){
    return this.list;
  }

  @Override
  public String getName() {
    return this.name;
  }


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


  @Override
  public boolean replaceAtOffset(Tuple t, long startOffset, long endOffset) {
    for(int i=0; i< list.size(); i++){
      if(list.get(i).getCommitLogEntry().getStartOffset() == startOffset &&
          list.get(i).getCommitLogEntry().getEndOffset() == endOffset){
        list.set(i, t);
        return true;
      }
    }
    return list.add(t);
  }
  
}
