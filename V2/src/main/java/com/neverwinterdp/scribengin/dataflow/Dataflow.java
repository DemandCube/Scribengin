package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.streamcoordinator.StreamCoordinator;

public interface Dataflow {
  //public DataflowConfig getDataflowConfig() ;

  //public void onModify(DataflowConfig config) ;
  
  public void setStreamCoordinator(StreamCoordinator s);
  
  public String getName();
  public void pause();
  public void stop();
  public void start();
  
  public void initScribes();
  public Scribe[] getScribes();
}

