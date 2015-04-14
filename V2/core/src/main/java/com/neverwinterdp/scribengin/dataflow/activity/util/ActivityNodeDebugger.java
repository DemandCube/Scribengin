package com.neverwinterdp.scribengin.dataflow.activity.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;

public class ActivityNodeDebugger implements NodeDebugger {
  
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = CREATE");
    ActivityNodeFormatter formater = new ActivityNodeFormatter(activityNode);
    System.out.println(formater.getFormattedText());
    registryDebugger.watchChild(activityNode.getPath() + "/activity-steps", ".*", formater);
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
