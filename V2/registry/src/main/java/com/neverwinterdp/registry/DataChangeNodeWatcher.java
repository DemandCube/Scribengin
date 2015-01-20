package com.neverwinterdp.registry;


abstract public class DataChangeNodeWatcher<T> extends NodeWatcher {
  protected Registry registry ;
  protected Class<T> dataType;
  
  public DataChangeNodeWatcher(Registry registry, Class<T> dataType) {
    this.registry = registry;
    this.dataType = dataType;
  }
  
  @Override
  public void process(NodeEvent event) {
    try {
      if(event.getType() == NodeEvent.Type.MODIFY || event.getType() == NodeEvent.Type.CREATE) {
        String path = event.getPath();
        T data = registry.getDataAs(path, dataType);
        onChange(event, data);
      } 
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }

  abstract public void onChange(NodeEvent event, T data) ;
}
