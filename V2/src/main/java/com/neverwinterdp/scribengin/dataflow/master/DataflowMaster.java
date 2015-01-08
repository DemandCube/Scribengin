package com.neverwinterdp.scribengin.dataflow.master;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.source.SourceFactory;
import com.neverwinterdp.vm.VMConfig;


public class DataflowMaster {
  @Inject
  private VMConfig vmConfig;
 
  @Inject
  private DataflowRegistry dataflowRegistry;
  
  @Inject
  private SourceFactory sourceFactory ;
  
  @Inject
  private SinkFactory sinkFactory ;
  
  private List<EventListener> listeners = new ArrayList<EventListener>();
  
  public VMConfig getVMConfig() { return this.vmConfig ; }
  
  public DataflowRegistry getDataflowRegistry() { return dataflowRegistry; }

  public SourceFactory getSourceFactory() { return sourceFactory; }

  public SinkFactory getSinkFactory() { return sinkFactory; }
  
  @Inject
  public void onInit() throws Exception {
    listeners.add(new InitEventListener());
    
    onEvent(EventListener.Event.INIT);
  }
  
  public void onDestroy() throws Exception {
  }
  
  private void onEvent(EventListener.Event event) throws Exception {
    for(int i = 0; i < listeners.size(); i++) {
      EventListener listener = listeners.get(i) ;
      listener.onEvent(this, event);
    }
  }
}
