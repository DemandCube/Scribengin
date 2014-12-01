package com.neverwinterdp.scribengin.stream.source;

import java.util.UUID;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.streamdescriptor.OffsetStreamDescriptor;
import com.neverwinterdp.scribengin.stream.streamdescriptor.StreamDescriptor;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class SequentialIntSourceStream implements SourceStream{
  int lastCommitted;
  int currNum;
  String name;
  
  public SequentialIntSourceStream(int startNum){
    this.currNum = startNum;
    this.lastCommitted = startNum;
    this.name = this.getClass().getSimpleName() +"-"+UUID.randomUUID().toString();
  }
  
  public SequentialIntSourceStream(OffsetStreamDescriptor sDesc){
    this.name = sDesc.getName();
    this.currNum = sDesc.getCurrentOffset();
    this.lastCommitted = sDesc.getLastCommittedOffset();
  }
  
  public SequentialIntSourceStream(){
    this(0);
  }
  

  @Override
  public boolean prepareCommit() {
    return true;
  }

  @Override
  public boolean commit() {
    return true;
  }

  @Override
  public boolean clearBuffer() {
    this.currNum = this.lastCommitted;
    return true;
  }

  @Override
  public boolean completeCommit() {
    this.lastCommitted = this.currNum;
    return true;
  }

  @Override
  public boolean rollBack() {
    this.currNum = this.lastCommitted;
    return true;
  }

  @Override
  public Tuple readNext() {
    this.currNum++;
    Tuple t = new Tuple(Integer.toString(this.currNum), 
                        Integer.toString(this.currNum).getBytes(),
                        new CommitLogEntry(this.getName(), this.currNum, this.currNum));
    return t;
  }

  @Override
  public boolean hasNext() {
    return this.currNum < Integer.MAX_VALUE;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public StreamDescriptor getStreamDescriptor() {
    return new OffsetStreamDescriptor(this.getName(), this.lastCommitted, this.currNum);
  }

}
