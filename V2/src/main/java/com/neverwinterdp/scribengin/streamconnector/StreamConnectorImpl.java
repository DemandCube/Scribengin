package com.neverwinterdp.scribengin.streamconnector;

import com.neverwinterdp.scribengin.commitlog.CommitLog;
import com.neverwinterdp.scribengin.commitlog.InMemoryCommitLog;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.source.SourceStream;
import com.neverwinterdp.scribengin.task.Task;
import com.neverwinterdp.scribengin.tuple.Tuple;
import com.neverwinterdp.scribengin.tuple.counter.InMemoryTupleCounter;
import com.neverwinterdp.scribengin.tuple.counter.TupleCounter;

public class StreamConnectorImpl implements StreamConnector{

  private SourceStream source;
  private SinkStream sink;
  private SinkStream invalidSink;
  private Task task;
  private CommitLog commitLog;
  private TupleCounter tupleTracker;
  

  public StreamConnectorImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t){
    this(y,z,invalidSink,t, new InMemoryTupleCounter());
  }
  
  public StreamConnectorImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, TupleCounter c){
    this.source = y;
    this.sink = z;
    this.invalidSink = invalidSink;
    task = t;
    commitLog = new InMemoryCommitLog();
    this.tupleTracker = c;    
  }
  
  public CommitLog getCommitLog(){
    return this.commitLog;
  }
  
  @SuppressWarnings("unused")
  private void setCommitLog(CommitLog c){
    this.commitLog = c;
  }

  @Override
  public void setTupleCounter(TupleCounter t) {
    this.tupleTracker = t;
  }
  
  @Override
  public boolean processNext() {
    long valid = 0;
    long invalid = 0;
    long created = 0;
    
    while(source.hasNext() && !task.readyToCommit()){
      Tuple[] tupleArray = task.execute(source.readNext());
      for(Tuple t : tupleArray){
        if(t.isInvalidData()){
          invalid++;
          invalidSink.append(t);
        }
        else{
          if(t.isTaskGenerated()){
            created++;
          }
          else{
            valid++;
          }
          sink.append(t);
        }
      }
    }
    
    //Some sort of lock on resources should likely happen here
    //lockResources(source,sink)
    
    //prepareCommit is a vote to make sure both sink, invalidSink, and source
    //are ready to commit data, otherwise rollback will occur
    //A single & is used to not short circuit the execution of the logical statement
    //http://stackoverflow.com/questions/8759868/java-logical-operator-short-circuiting
    if(sink.prepareCommit() & source.prepareCommit() & invalidSink.prepareCommit()){
      long numTuplesWritten = sink.getBufferSize() + invalidSink.getBufferSize();
      //The actual committing of data
      if(sink.commit() & invalidSink.commit()){
        //update any offsets that need to be managed
        invalidSink.updateOffSet();
        sink.updateOffSet();
        source.updateOffSet();
        
        
        this.tupleTracker.addCreated(created);
        this.tupleTracker.addInvalid(invalid);
        this.tupleTracker.addValid(valid);
        this.tupleTracker.addWritten(numTuplesWritten);
        
       //send some sort of acknowledgement?
       //write to a commitLog?
      }
      else{
        //Undo anything that could have gone wrong, 
        //delete data, etc
        source.clearCommit();
        sink.rollBack();
        invalidSink.rollBack();
      }
    }
    else{
      //Clean up everything 
      sink.clearCommit();
      source.clearCommit();
      invalidSink.clearCommit();
    }
    
    //If resources are locked, unlock at end
    //releaseLocks(source,sink)
    
    return true;
  }

  @Override
  public Task getTask(){
    return this.task;
  }

  @Override
  public SinkStream getSinkStream() {
    return this.sink;
  }

  @Override
  public SinkStream getInvalidSink() {
    return this.invalidSink;
  }


  @Override
  public SourceStream getSourceStream() {
    return this.source;
  }
  
  @Override
  public void setInvalidSink(SinkStream s) {
    this.invalidSink = s;
  }
  
  @Override
  public void setSourceStream(SourceStream s) {
    this.source = s;
  }

  @Override
  public void setSink(SinkStream s) {
    this.sink = s;
  }

  @Override
  public void setTask(Task t) {
    this.task = t;
  }
  
  @Override
  public TupleCounter getTupleTracker() {
    return this.tupleTracker;
  }
  
/*
  @Override
  public boolean verifyDataInSink() {
    CommitLogEntry[] commitLogs = this.commitLog.getCommitLogs();
    
    
    boolean isDataValid = true;
    
    for(int i =0; i < commitLogs.length; i++){
      if(!commitLogs[i].isInvalidData()){
        if(! this.source.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()).equals(
                    this.sink.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()))
                  ) {
          isDataValid = false;
          break;
        }
      }
      else{
        if(! this.source.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()).equals(
            this.invalidSink.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()))
          ) {
          isDataValid = false;
          break;
        }
      }
    }
    
    return isDataValid;
  }

  @Override
  public boolean fixDataInSink(){
    CommitLogEntry[] commitLogs = this.commitLog.getCommitLogs();
    for(int i =0; i < commitLogs.length; i++){
      if(!commitLogs[i].isInvalidData()){
        if(! this.source.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()).equals(
            this.sink.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()))
          ) {
          this.sink.replaceAtOffset(
              this.source.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()), 
              commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset());
        }
      }
      else{
        if(! this.source.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()).equals(
            this.invalidSink.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()))
          ) {
          this.invalidSink.replaceAtOffset(
              this.source.readFromOffset(commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset()), 
              commitLogs[i].getStartOffset(), commitLogs[i].getEndOffset());
        }
      }
    }
    return this.verifyDataInSink();
  }

*/

  

}
