package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.streamconnector.StreamConnector;

public class ScribeImpl implements Scribe{

  private StreamConnector streamConnector;
  private int timeout;
  private boolean active;
  private Thread scribeThread;

  public ScribeImpl(StreamConnector s){
    this(s, 1000);
  }
  
  public ScribeImpl(StreamConnector s, int Timeout){
    this.streamConnector = s;
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
  public void setStream(StreamConnector s) {
    streamConnector = s;
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
        streamConnector.processNext();
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
    return true;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public StreamConnector getStreamConnector() {
    return this.streamConnector;
  }

}
