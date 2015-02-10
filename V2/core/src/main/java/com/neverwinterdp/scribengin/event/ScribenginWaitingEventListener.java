package com.neverwinterdp.scribengin.event;

import static com.neverwinterdp.scribengin.event.ScribenginEvent.*;

import com.neverwinterdp.registry.DataChangeNodeWatcher;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.vm.event.VMWaitingEventListener;


public class ScribenginWaitingEventListener extends VMWaitingEventListener {
  public ScribenginWaitingEventListener(Registry registry) throws RegistryException {
    super(registry);
    registryListener.watch(ScribenginService.LEADER_PATH, new VMLeaderElectedNodeWatcher(registry), true);
  }
  
  public void waitScribenginMaster(String desc, String vmName) throws Exception {
    add(new VMMasterElectionEventListener(desc, vmName));
  }
  
  public void waitDataflowLeader(String desc, String dataflowName, String vmName) throws Exception {
    add(new VMMasterElectionEventListener(desc, vmName));
    String dataflowLeaderPath = ScribenginService.getDataflowLeaderPath(dataflowName);
    registryListener.watch(dataflowLeaderPath, new VMLeaderElectedNodeWatcher(registry));
  }
  
  public void waitDataflowStatus(String desc, String dataflowName, DataflowLifecycleStatus status) throws Exception {
    add(new DataflowStatusEventListener(desc, dataflowName, status));
    String dataflowStatusPath = ScribenginService.getDataflowStatusPath(dataflowName);
    DataChangeNodeWatcher<DataflowLifecycleStatus> dataflowStatusWatcher = new DataChangeNodeWatcher<DataflowLifecycleStatus>(registry, DataflowLifecycleStatus.class) {
      @Override
      public void onChange(NodeEvent event, DataflowLifecycleStatus data) {
        try {
          ScribenginEvent sEvent = new ScribenginEvent(DATAFLOW_STATUS, event);
          Node statusNode = new Node(registry, event.getPath());
          Node dataflowNode = statusNode.getParentNode();
          DataflowDescriptor dfDescriptor = dataflowNode.getDataAs(DataflowDescriptor.class);
          sEvent.attr(DataflowAttr.status, data);
          sEvent.attr(DataflowAttr.descriptor, dfDescriptor);
          ScribenginWaitingEventListener.this.process(sEvent);
        } catch(Exception ex) {
          ex.printStackTrace();
        }
      }
    };
    registryListener.watch(dataflowStatusPath, dataflowStatusWatcher, true);
  }
  
  static public class DataflowStatusEventListener extends ScribenginEventListener {
    String   dataflowName;
    DataflowLifecycleStatus status;
    
    public DataflowStatusEventListener(String desc, String dataflowName, DataflowLifecycleStatus status) {
      super(desc);
      this.dataflowName = dataflowName;
      this.status = status;
    }

    @Override
    public boolean process(ScribenginEvent event) {
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