package com.neverwinterdp.scribengin.scribe;

import java.util.LinkedList;
import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;

import org.apache.log4j.Logger;

public class TransactionScribe implements ScribeInterface{

  protected Logger LOG;
  List<Record> bufferedRecords;
  int bufferLimit;
  int bufferTimeout;
  long lastCommitTime;
  ScribeState state;
  
  public TransactionScribe(){
    LOG = Logger.getLogger(this.getClass().getName());
    this.bufferedRecords = new LinkedList<Record>();
    this.bufferLimit = 100;
    this.bufferTimeout = 30000;
    this.lastCommitTime = System.currentTimeMillis();
    
    this.setState(ScribeState.INIT);
  }
  
  @Override
  public void process(Record record, DataflowTaskContext ctx) throws Exception {
    //Read in data from source, add to sink's buffer
    if(ctx.append(record)){
      return;
    }

    
    //prepareCommit is a vote to make sure both sink, invalidSink, and source
    //are ready to commit data, otherwise rollback will occur
    if(ctx.prepareCommit()){
      //The actual committing of data
      if(ctx.commit()){
        //update any offsets that need to be managed, clear temp data
        ctx.completeCommit();
      }
      else{
        //Undo anything that could have gone wrong,
        //undo any commits, go back as if nothing happened
        ctx.rollback();
      }
    }
    else{
      //Clean up everything
      ctx.clearBuffer();
    }
  }
  
  
  
  public void setState(ScribeState s){
    this.state = s;
  }

  public ScribeState getState(){
    return this.state;
  }
}
