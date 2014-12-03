package com.neverwinterdp.scribengin.scribe;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.commitlog.CommitLog;
import com.neverwinterdp.scribengin.commitlog.InMemoryCommitLog;
import com.neverwinterdp.scribengin.scribe.state.InMemoryScribeStateTracker;
import com.neverwinterdp.scribengin.scribe.state.ScribeState;
import com.neverwinterdp.scribengin.scribe.state.ScribeStateTracker;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.source.SourceStream;
import com.neverwinterdp.scribengin.task.Task;
import com.neverwinterdp.scribengin.tuple.Tuple;
import com.neverwinterdp.scribengin.tuple.counter.InMemoryTupleCounter;
import com.neverwinterdp.scribengin.tuple.counter.TupleCounter;

public class ScribeImpl implements Scribe{

  private boolean active;
  private CommitLog commitLog;
  private SinkStream invalidSink;
  private int processNextTimeout;
  private int retryTimeoutTimeLimit;
  private Thread scribeThread;
  private SinkStream sink;
  private SourceStream source;
  //private ScribeState myState;
  private ScribeStateTracker stateTracker;
  private Task task;
  private TupleCounter tupleTracker;
  private Logger LOG;
  
  
  public ScribeImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t){
    this(y,z,invalidSink,t, new InMemoryTupleCounter(), new InMemoryScribeStateTracker(), 600000, 500);
  }
  
  public ScribeImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, int timeout){
    this(y,z,invalidSink,t, new InMemoryTupleCounter(), new InMemoryScribeStateTracker(), 600000, 500);
  }
  
  public ScribeImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, ScribeStateTracker sst){
    this(y,z,invalidSink,t, new InMemoryTupleCounter(), sst, 600000, 500);
  }
  
  public ScribeImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, TupleCounter c){
    this(y,z,invalidSink,t, c, new InMemoryScribeStateTracker(), 600000, 500);
  }
  
  public ScribeImpl(SourceStream y, SinkStream z, SinkStream invalidSink, Task t, TupleCounter c, ScribeStateTracker sst, int retryTimeoutTimeLimit, int processNextTimeout){
    this.source = y;
    this.sink = z;
    this.invalidSink = invalidSink;
    this.task = t;
    this.commitLog = new InMemoryCommitLog();
    this.tupleTracker = c;
    this.retryTimeoutTimeLimit = retryTimeoutTimeLimit;
    this.processNextTimeout = processNextTimeout;
    this.stateTracker = sst;
    
    this.active = false;
    
    LOG = Logger.getLogger(this.getClass().getName());
  }
  
  private void buffer() throws NoSuchMethodException {
    //ScribeState currState = this.getState();
    //if(!(currState == ScribeState.INIT || currState == ScribeState.ERROR || 
    //    currState == ScribeState.CLEARING_BUFFER || currState == ScribeState.COMPLETING_COMMIT || 
    //    currState == ScribeState.ROLLINGBACK || currState == ScribeState.STOPPED )){
    //  return;
    //}
    
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

  private void clearBuffer() throws NoSuchMethodException {
    //if(this.getState() != ScribeState.PREPARING_COMMIT){
    //  return;
    //}
    
    this.setState(ScribeState.CLEARING_BUFFER);
    this.tupleTracker.clearBuffer();
    this.gradualBackoff(sink, sink.getClass().getMethod("clearBuffer"));
    this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("clearBuffer"));
    this.gradualBackoff(source, source.getClass().getMethod("clearBuffer"));
    this.gradualBackoff(task, task.getClass().getMethod("commit"));
  }

  private boolean commit() throws NoSuchMethodException {
    //if(this.getState() != ScribeState.PREPARING_COMMIT){
    //  return false;
    //}
    
    this.setState(ScribeState.COMMITTING);
    this.tupleTracker.addWritten(sink.getBufferSize() + invalidSink.getBufferSize());
    return this.gradualBackoff(sink, sink.getClass().getMethod("commit")) & 
        this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("commit"));
  }
  
  private void completeCommit() throws NoSuchMethodException {
    //if(this.getState() != ScribeState.COMMITTING){
    //  return;
    //}
    
    this.setState(ScribeState.COMPLETING_COMMIT);
    this.gradualBackoff(sink, sink.getClass().getMethod("completeCommit"));
    this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("completeCommit"));
    this.gradualBackoff(source, source.getClass().getMethod("completeCommit"));
    this.gradualBackoff(task, task.getClass().getMethod("commit"));
    this.tupleTracker.commit();
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
          LOG.error("Scribe's sleep has been interrupted: " + e.getMessage());
          //e.printStackTrace();
        }
      }
    }
  }

  public CommitLog getCommitLog(){
    return this.commitLog;
  }

  @Override
  public SinkStream getInvalidSink() {
    return this.invalidSink;
  }

  //Only to be used for testing.
  @SuppressWarnings("unused")
  private ScribeState killScribeThread(){
    this.scribeThread.interrupt();
    this.active = false;
    return this.stateTracker.getScribeState();
  }

  @Override
  public SinkStream getSinkStream() {
    return this.sink;
  }

  @Override
  public SourceStream getSourceStream() {
    return this.source;
  }

  @Override
  public ScribeState getState(){
    //return this.myState;
    return stateTracker.getScribeState();
  }


  @Override
  public Task getTask(){
    return this.task;
  }

  @Override
  public TupleCounter getTupleTracker() {
    return this.tupleTracker;
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
        LOG.error(e.getMessage());
        return false;
      }
      
    }
    return x;
  }

  @Override
  public boolean init() {
    return this.init(ScribeState.INIT);
  }


  @Override
  public boolean init(ScribeState state) {
    this.setState(state);
    scribeThread = new Thread() {
      public void run() {
        try{
          consumeLoop() ;
        }
          catch (Exception e) {
            LOG.error(e.getMessage());
            //e.printStackTrace();
        }
      }
    };
    scribeThread.start();
    
    return true;
  }
  
  private boolean prepareCommit() throws NoSuchMethodException {
    //if(this.getState() != ScribeState.BUFFERING){
    //  return false;
    //}
    
    this.setState(ScribeState.PREPARING_COMMIT);
    return this.gradualBackoff(sink, sink.getClass().getMethod("prepareCommit")) & 
        this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("prepareCommit")) & 
        this.gradualBackoff(source, source.getClass().getMethod("prepareCommit"));
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
      LOG.error(e.getMessage());
      //e.printStackTrace();
    }
    
    //If resources are locked, unlock at end
    //releaseLocks(source,sink)
    
    return retVal;
  }

  @Override
  public boolean recover() {
    ScribeState state = this.getState();
    //System.err.print("RECOVER STATE: ");
    //System.err.println(state);
    try {
      switch(state){
        case UNINITIALIZED:
          return true;
        case INIT:
          return true;
        case ERROR:
          return true;
        case STOPPED:
          return true;
          
        case BUFFERING:
          this.clearBuffer();
          return true;
        case PREPARING_COMMIT:
          this.clearBuffer();
          return true;
        case CLEARING_BUFFER:
          this.clearBuffer();
          return true;
          
        case COMMITTING:
          this.rollBack();
          return true;
        case COMPLETING_COMMIT:
          this.rollBack();
          return true;
        case ROLLINGBACK:
          this.rollBack();
          return true;
        default:
          return false;
      }
    } catch (NoSuchMethodException e) {
      LOG.error(e.getMessage());
      //e.printStackTrace();
      return false;
    }
    //return false;
  }

  private void rollBack() throws NoSuchMethodException {
    //if(this.getState() != ScribeState.COMMITTING){
    //  return;
    //}
    
    this.setState(ScribeState.ROLLINGBACK);
    this.tupleTracker.clearBuffer();
    this.gradualBackoff(sink, sink.getClass().getMethod("rollBack"));
    this.gradualBackoff(invalidSink, invalidSink.getClass().getMethod("rollBack"));
    this.gradualBackoff(source, source.getClass().getMethod("rollBack"));
    this.gradualBackoff(task, task.getClass().getMethod("commit"));
  }
  
  @SuppressWarnings("unused")
  private void setCommitLog(CommitLog c){
    this.commitLog = c;
  }

  
  @Override
  public void setInvalidSink(SinkStream s) {
    this.invalidSink = s;
  }
  
  @Override
  public void setSink(SinkStream s) {
    this.sink = s;
  }

  @Override
  public void setSourceStream(SourceStream s) {
    this.source = s;
  }
  

  @Override
  public void setState(ScribeState s){
    LOG.debug("Setting state: "+ s.toString());
    stateTracker.setState(s);
  }

  @Override
  public void setTask(Task t) {
    this.task = t;
  }
  
  
  @Override
  public void setTupleCounter(TupleCounter t) {
    this.tupleTracker = t;
  }
  
  @Override
  public void start() {
    LOG.info("Start - Setting active to true");
    active = true;
  }

  @Override
  public void stop() {
    LOG.info("Stop - Setting active to true");
    active = false;
  }
}

