package com.neverwinterdp.scribengin.client;

import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowMaster;

public class ScribenginClient {
  private Registry registry;

  public ScribenginClient(Registry registry) {
    this.registry = registry;
  }

  public Registry getRegistry() { return this.registry; }
  
  public List<DataflowDescriptor> getDataflowDescriptor() throws RegistryException {
    return registry.getChildrenAs(DataflowMaster.SCRIBENGIN_DATAFLOWS, DataflowDescriptor.class) ;
  }
}
