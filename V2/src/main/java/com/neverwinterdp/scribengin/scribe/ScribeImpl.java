package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.stream.Stream;
import com.neverwinterdp.scribengin.task.Task;

public class ScribeImpl implements Scribe{

  private Stream stream;
  private int timeout;
  private boolean active;
  private Thread scribeThread;

  public ScribeImpl(Stream s){
    this(s, 1000);
  }
  
  public ScribeImpl(Stream s, int Timeout){
    this.stream = s;
    timeout = Timeout;
    active = false;
    
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
  }
  
  @Override
  public void setStream(Stream s) {
    stream = s;
  }

  @Override
  public void start() {
    active = true;
  }

  @Override
  public void stop() {
    active = false;
  }
  
  private void consumeLoop() {
    while(true){
      if(active){
        stream.processNext();
      }
      else{
        try {
          Thread.sleep(timeout);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public boolean init() {
    return this.stream.initStreams();
  }

  @Override
  public boolean close() {
    return this.stream.closeStreams();
  }

  @Override
  public Stream getStream() {
    return this.stream;
  }

}
