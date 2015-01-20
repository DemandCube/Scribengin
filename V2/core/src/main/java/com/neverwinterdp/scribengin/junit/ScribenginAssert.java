package com.neverwinterdp.scribengin.junit;

import static com.neverwinterdp.scribengin.junit.ScribenginAssertEvent.*;

import com.neverwinterdp.registry.DataChangeNodeWatcher;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.vm.junit.VMAssert;


public class ScribenginAssert extends VMAssert {
  public ScribenginAssert(Registry registry) throws RegistryException {
    super(registry);
    registryListener.watch(ScribenginService.LEADER_PATH, new VMLeaderElectionNodeWatcher(registry), true);
  }
  
  public void watchDataflow(String dataflowName) throws RegistryException {
    String dataflowLeaderPath = ScribenginService.getDataflowLeaderPath(dataflowName);
    registryListener.watch(dataflowLeaderPath, new VMLeaderElectionNodeWatcher(registry), true);
    
    String dataflowStatusPath = ScribenginService.getDataflowStatusPath(dataflowName);
    DataChangeNodeWatcher<DataflowLifecycleStatus> dataflowStatusWatcher = new DataChangeNodeWatcher<DataflowLifecycleStatus>(registry, DataflowLifecycleStatus.class) {
      @Override
      public void onChange(NodeEvent event, DataflowLifecycleStatus data) {
        try {
          ScribenginAssertEvent sEvent = new ScribenginAssertEvent(DATAFLOW_STATUS, event);
          Node statusNode = new Node(registry, event.getPath());
          Node dataflowNode = statusNode.getParentNode();
          DataflowDescriptor dfDescriptor = dataflowNode.getData(DataflowDescriptor.class);
          sEvent.attr(DataflowAttr.status, data);
          sEvent.attr(DataflowAttr.descriptor, dfDescriptor);
          assertEvent(sEvent);
        } catch(Exception ex) {
          ex.printStackTrace();
        }
      }
    };
    registryListener.watch(dataflowStatusPath, dataflowStatusWatcher, true);
  }
  
  public void assertScribenginMaster(String desc, String vmName) throws Exception {
    add(new VMAssertMasterElection(desc, vmName));
  }
  
  public void assertDataflowMaster(String desc, String vmName) throws Exception {
    add(new VMAssertMasterElection(desc, vmName));
  }
  
  public void assertDataflowStatus(String desc, String dataflowName, DataflowLifecycleStatus status) throws Exception {
    add(new AssertDataflowStatus(desc, dataflowName, status));
  }
  
  static public class AssertDataflowStatus extends ScribenginAssertUnit {
    String   dataflowName;
    DataflowLifecycleStatus status;
    
    public AssertDataflowStatus(String desc, String dataflowName, DataflowLifecycleStatus status) {
      super(desc);
      this.dataflowName = dataflowName;
      this.status = status;
    }

    @Override
    public boolean assertEvent(ScribenginAssertEvent event) {
      if(!DATAFLOW_STATUS.equals(event.getName())) return false;
      DataflowDescriptor descriptor = event.attr(DataflowAttr.descriptor) ;
      if(descriptor == null) return false;
      if(!dataflowName.equals(descriptor.getName())) return false;
      DataflowLifecycleStatus status = event.attr(DataflowAttr.status) ;
      if(!this.status.equals(status)) return false;
      return true;
    }
  }
}