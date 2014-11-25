package com.neverwinterdp.scribengin.streamconnector;

import com.neverwinterdp.scribengin.commitlog.CommitLog;
import com.neverwinterdp.scribengin.commitlog.InMemoryCommitLog;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.source.SourceStream;
import com.neverwinterdp.scribengin.task.Task;
import com.neverwinterdp.scribengin.tuple.Tuple;
import com.neverwinterdp.scribengin.tuple.counter.InMemoryTupleCounter;
import com.neverwinterdp.scribengin.tuple.counter.TupleCounter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class StreamConnectorImpl implements StreamConnector{

  private SourceStream source;
  private SinkStream sink;
  private SinkStream invalidSink;
  private Task task;
  private CommitLog commitLog;
  private TupleCounter tupleTracker;
  private int retryTimeoutTimeLimit;

  public StreamConnectorImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t){
    this(y,z,invalidSink,t, new InMemoryTupleCounter(), 600000);
  }
  
  public StreamConnectorImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, TupleCounter c){
    this(y,z,invalidSink,t, c, 600000);
  }
  
  public StreamConnectorImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, TupleCounter c, int retryTimeoutTimeLimit){
    this.source = y;
    this.sink = z;
    this.invalidSink = invalidSink;
    this.task = t;
    this.commitLog = new InMemoryCommitLog();
    this.tupleTracker = c;
    this.retryTimeoutTimeLimit = retryTimeoutTimeLimit;
  }
  
  
  
  @Override
  public boolean processNext() {
    long valid = 0;
    long invalid = 0;
    long created = 0;
    boolean retVal = true;
    
    try {
      while(source.hasNext() && !task.readyToCommit()){
        Tuple[] tupleArray = task.execute(source.readNext());
        for(Tuple t : tupleArray){
          if(t.isInvalidData()){
            invalid++;
            this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("append", Tuple.class), t);
          }
          else{
            if(t.isTaskGenerated()){
              created++;
            }
            else{
              valid++;
            }
            this.gradualBackoff(sink, sink.getClass().getMethod("append", Tuple.class), t);
          }
        }
      }
    
      //Some sort of lock on resources should likely happen here
      //lockResources(source,sink)
      
      //prepareCommit is a vote to make sure both sink, invalidSink, and source
      //are ready to commit data, otherwise rollback will occur
      //A single & is used to not short circuit the execution of the logical statement
      //http://stackoverflow.com/questions/8759868/java-logical-operator-short-circuiting
      if(this.gradualBackoff(sink, sink.getClass().getMethod("prepareCommit")) & 
          this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("prepareCommit")) & 
          this.gradualBackoff(source, source.getClass().getMethod("prepareCommit"))){
        
        long numTuplesWritten = sink.getBufferSize() + invalidSink.getBufferSize();
        
        
        //The actual committing of data
        if(this.gradualBackoff(sink, sink.getClass().getMethod("commit")) & 
            this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("commit"))){
          
          //update any offsets that need to be managed
          this.gradualBackoff(sink, sink.getClass().getMethod("updateOffSet"));
          this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("updateOffSet"));
          this.gradualBackoff(source, source.getClass().getMethod("updateOffSet"));
          
          this.tupleTracker.addCreated(created);
          this.tupleTracker.addInvalid(invalid);
          this.tupleTracker.addValid(valid);
          this.tupleTracker.addWritten(numTuplesWritten);
          
         //send some sort of acknowledgement?
         //write to a commitLog?
        }
        else{
          //Undo anything that could have gone wrong, 
          this.gradualBackoff(sink, sink.getClass().getMethod("rollBack"));
          this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("rollBack"));
          this.gradualBackoff(source, source.getClass().getMethod("clearCommit"));
        }
      }
      else{
        //Clean up everything
        this.gradualBackoff(sink, sink.getClass().getMethod("clearCommit"));
        this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("clearCommit"));
        this.gradualBackoff(source, source.getClass().getMethod("clearCommit"));
      }
    } catch (NoSuchMethodException | SecurityException e) {
      e.printStackTrace();
    }
    
    //If resources are locked, unlock at end
    //releaseLocks(source,sink)
    
    return retVal;
  }


  private boolean gradualBackoff(Object o, Method m){
    return this.gradualBackoff(o, m, null);
  }

  /**
   * 
   * @param o
   * @param m
   * @param args
   * @return
   */
  private boolean gradualBackoff(Object o, Method m, Object args){
    boolean x = false;
    int backoff = 1;

    while(!x){
      try {
        if(args == null){
          x = (boolean) m.invoke(o);
        }
        else{
          //Arbitrarily cast the args object to correct type
          x = (boolean) m.invoke(o, args.getClass().cast(args));
        }
        if(!x){
          //Cut it off @ retryTimeoutTimeLimit
          if(backoff > this.retryTimeoutTimeLimit){
            return false;
          }
          Thread.sleep(backoff);
          backoff = backoff * 10;
        }
      } catch (IllegalAccessException | IllegalArgumentException
          | InvocationTargetException | InterruptedException e) {
        e.printStackTrace();
        return false;
      }
      
    }
    return x;
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
}

