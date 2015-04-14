package com.neverwinterdp.scribengin.dataflow.activity.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;

/**
 * TODO: the activity can be executed very fast and finished before the client can can register and watch for the event
 * For now, we work around by detect the delete event and print out the activity in the history.
 *  
 * This debugger should watch the /path/to/activities/active to detect the new activity. 
 * @author Tuan
 */
public class ActiveActivityNodeDebugger implements NodeDebugger {

  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActiveActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = CREATE");
    ActivityNodeFormatter formater = new ActivityNodeFormatter(activityNode);
    registryDebugger.println(formater.getFormattedText());
    registryDebugger.watchChild(activityNode.getPath() + "/activity-steps", ".*", formater);
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActiveActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = MODIFY");
    ActivityNodeFormatter formater = new ActivityNodeFormatter(activityNode);
    registryDebugger.println(formater.getFormattedText());
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActiveActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = DELETE");
    String path = activityNode.getPath();
    path = path.replace("/active/", "/history/");
    Node historyNode = activityNode.getRegistry().get(path);
    ActivityNodeFormatter formater = new ActivityNodeFormatter(historyNode);
    registryDebugger.println(formater.getFormattedText());
  }

}
