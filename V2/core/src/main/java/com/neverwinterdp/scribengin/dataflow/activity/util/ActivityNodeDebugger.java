package com.neverwinterdp.scribengin.dataflow.activity.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.registry.util.RegistryDebugger;

public class ActivityNodeDebugger implements NodeDebugger {
  boolean detailedDebugger;

  public ActivityNodeDebugger(boolean detailedDebugger){
    this.detailedDebugger = detailedDebugger;
  }

  public ActivityNodeDebugger(){
    this(false);
  }
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = CREATE");
    NodeFormatter formatter = null;
    if(this.detailedDebugger){
      formatter = new ActivityNodeDetailedFormatter(activityNode);
    }
    else{
      formatter = new ActivityNodeSimpleFormatter(activityNode);
    }
    registryDebugger.watchChild(activityNode.getPath() + "/activity-steps", ".*", formatter);
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = MODIFY");
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = DELETE");
  }

}
