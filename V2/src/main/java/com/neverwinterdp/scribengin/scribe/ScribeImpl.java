package com.neverwinterdp.scribengin.scribe;

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

public class ScribeImpl implements Scribe{

  private SourceStream source;
  private SinkStream sink;
  private SinkStream invalidSink;
  private Task task;
  private CommitLog commitLog;
  private TupleCounter tupleTracker;
  private int retryTimeoutTimeLimit;
  private Thread scribeThread;
  private boolean active;
  private int processNextTimeout;
  private ScribeState myState;
  
  
  public ScribeImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t){
    this(y,z,invalidSink,t, new InMemoryTupleCounter(), 600000, 1000);
  }
  
  public ScribeImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, int timeout){
    this(y,z,invalidSink,t, new InMemoryTupleCounter(), 600000, 1000);
  }
  
  public ScribeImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, TupleCounter c){
    this(y,z,invalidSink,t, c, 600000, 1000);
  }
  
  public ScribeImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, TupleCounter c, int retryTimeoutTimeLimit, int processNextTimeout){
    this.source = y;
    this.sink = z;
    this.invalidSink = invalidSink;
    this.task = t;
    this.commitLog = new InMemoryCommitLog();
    this.tupleTracker = c;
    this.retryTimeoutTimeLimit = retryTimeoutTimeLimit;
    this.processNextTimeout = processNextTimeout;
    
    this.active = false;
    this.myState = ScribeState.UNINITIALIZED; 
  }
  
  

  private void consumeLoop() {
    while(true){
      if(active){
        this.processNext();
      }
      else{
        try {
          this.setState(ScribeState.STOPPED);
          Thread.sleep(processNextTimeout);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public boolean init() {
    return this.init(ScribeState.INIT);
  }
  
  @Override
  public boolean init(ScribeState state) {
    scribeThread = new Thread() {
      public void run() {
        try{
          consumeLoop() ;
        }
          catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    scribeThread.start();
    this.setState(state);
    return true;
  }
  
  @Override
  public boolean processNext() {
    boolean retVal = true;

    try {
      //Read in data from source, add to sink's buffer
      buffer();

      //Some sort of lock on resources should likely happen here
      //lockResources(source,sink)
      
      //prepareCommit is a vote to make sure both sink, invalidSink, and source
      //are ready to commit data, otherwise rollback will occur
      //A single & is used to not short circuit the execution of the logical statement
      //http://stackoverflow.com/questions/8759868/java-logical-operator-short-circuiting
      if(prepareCommit()){
        //The actual committing of data
        if(commit()){
          //update any offsets that need to be managed, clear temp data
          completeCommit();
          
         //send some sort of acknowledgement?
         //write to a commitLog?
        }
        else{
          //Undo anything that could have gone wrong,
          //undo any commits, go back as if nothing happened
          rollBack();
        }
      }
      else{
        //Clean up everything
        clearBuffer();
      }
    } catch (NoSuchMethodException | SecurityException e) {
      this.setState(ScribeState.ERROR);
      e.printStackTrace();
    }
    
    //If resources are locked, unlock at end
    //releaseLocks(source,sink)
    
    return retVal;
  }

  private void clearBuffer() throws NoSuchMethodException {
    if(this.getState() != ScribeState.PREPARING_COMMIT){
      return;
    }
    
    this.setState(ScribeState.CLEARING_BUFFER);
    this.tupleTracker.clearBuffer();
    this.gradualBackoff(sink, sink.getClass().getMethod("clearBuffer"));
    this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("clearBuffer"));
    this.gradualBackoff(source, source.getClass().getMethod("clearBuffer"));
    this.gradualBackoff(task, task.getClass().getMethod("commit"));
  }

  private void rollBack() throws NoSuchMethodException {
    if(this.getState() != ScribeState.COMMITTING){
      return;
    }
    
    this.setState(ScribeState.ROLLINGBACK);
    this.tupleTracker.clearBuffer();
    this.gradualBackoff(sink, sink.getClass().getMethod("rollBack"));
    this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("rollBack"));
    this.gradualBackoff(source, source.getClass().getMethod("rollBack"));
    this.gradualBackoff(task, task.getClass().getMethod("commit"));
  }

  private void completeCommit() throws NoSuchMethodException {
    if(this.getState() != ScribeState.COMMITTING){
      return;
    }
    
    this.setState(ScribeState.COMPLETING_COMMIT);
    this.gradualBackoff(sink, sink.getClass().getMethod("completeCommit"));
    this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("completeCommit"));
    this.gradualBackoff(source, source.getClass().getMethod("completeCommit"));
    this.gradualBackoff(task, task.getClass().getMethod("commit"));
    this.tupleTracker.commit();
  }

  private boolean commit() throws NoSuchMethodException {
    if(this.getState() != ScribeState.PREPARING_COMMIT){
      return false;
    }
    
    this.setState(ScribeState.COMMITTING);
    this.tupleTracker.addWritten(sink.getBufferSize() + invalidSink.getBufferSize());
    return this.gradualBackoff(sink, sink.getClass().getMethod("commit")) & 
        this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("commit"));
  }

  private void buffer() throws NoSuchMethodException {
    ScribeState currState = this.getState();
    if(!(currState == ScribeState.INIT || currState == ScribeState.ERROR || 
        currState == ScribeState.CLEARING_BUFFER || currState == ScribeState.COMPLETING_COMMIT || 
        currState == ScribeState.ROLLINGBACK || currState == ScribeState.STOPPED )){
      return;
    }
    
    this.setState(ScribeState.BUFFERING);
    while(source.hasNext() && !task.readyToCommit()){
      Tuple[] tupleArray = task.execute(source.readNext());
      for(Tuple t : tupleArray){
        if(t.isInvalidData()){
          this.tupleTracker.incrementInvalid();
          this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("bufferTuple", Tuple.class), t);
        }
        else{
          if(t.isTaskGenerated()){
            this.tupleTracker.incrementCreated();
          }
          else{
            this.tupleTracker.incrementValid();
          }
          this.gradualBackoff(sink, sink.getClass().getMethod("bufferTuple", Tuple.class), t);
        }
      }
    }
  }

  private boolean prepareCommit() throws NoSuchMethodException {
    if(this.getState() != ScribeState.BUFFERING){
      return false;
    }
    
    this.setState(ScribeState.PREPARING_COMMIT);
    return this.gradualBackoff(sink, sink.getClass().getMethod("prepareCommit")) & 
        this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("prepareCommit")) & 
        this.gradualBackoff(source, source.getClass().getMethod("prepareCommit"));
  }


  private boolean gradualBackoff(Object o, Method m){
    return this.gradualBackoff(o, m, null);
  }

  /**
   * Does a backoff on executing 
   * @param o Object that contains Method m
   * @param m The Method to execute
   * @param args Any arguments to pass into the method (Automagically get cast correctly)
   * @return true or false.  Returns true immediately once true is returned
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
  

  @Override
  public void start() {
    active = true;
  }

  @Override
  public void stop() {
    active = false;
  }
  
  
  @Override
  public ScribeState getState(){
    return this.myState;
  }
  
  @Override
  public void setState(ScribeState s){
    this.myState  = s;
  }
}

