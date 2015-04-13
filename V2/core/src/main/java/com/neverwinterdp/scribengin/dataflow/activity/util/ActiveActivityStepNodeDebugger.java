package com.neverwinterdp.scribengin.dataflow.activity.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;

public class ActiveActivityStepNodeDebugger implements NodeDebugger {
  
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node activityStepNode) throws Exception {
    registryDebugger.println("ActiveActivityStepNodeDebugger: Node = " + activityStepNode.getPath() + ", Event = CREATE");
    ActiveActivityStepRegistryFormatter formatter = new ActiveActivityStepRegistryFormatter(activityStepNode);
    registryDebugger.println(formatter.getFormattedText());
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node activityStepNode) throws Exception {
    registryDebugger.println("ActiveActivityStepNodeDebugger: Node = " + activityStepNode.getPath() + ", Event = MODIFY");
    ActiveActivityStepRegistryFormatter formatter = new ActiveActivityStepRegistryFormatter(activityStepNode);
    registryDebugger.println(formatter.getFormattedText());
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node activityStepNode) throws Exception {
    registryDebugger.println("ActiveActivityStepNodeDebugger: Node = " + activityStepNode.getPath() + ", Event = DELETE");
  }

}
