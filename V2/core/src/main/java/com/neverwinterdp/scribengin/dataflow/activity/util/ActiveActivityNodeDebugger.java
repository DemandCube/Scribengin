package com.neverwinterdp.scribengin.dataflow.activity.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.registry.util.RegistryDebugger;

/**
 * TODO: the activity can be executed very fast and finished before the client can can register and watch for the event
 * For now, we work around by detect the delete event and print out the activity in the history.
 *  
 * This debugger should watch the /path/to/activities/active to detect the new activity. 
 * @author Tuan
 */
public class ActiveActivityNodeDebugger implements NodeDebugger {
  boolean detailedDebugger;

  public ActiveActivityNodeDebugger(boolean detailedDebugger){
    this.detailedDebugger = detailedDebugger;
  }

  public ActiveActivityNodeDebugger(){
    this(false);
  }
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActiveActivityNodeDebugger: Node = " + activityNode.getPath() + ", TerminateEvent = CREATE");
    NodeFormatter formatter = null;
    if(this.detailedDebugger){
      formatter = new ActivityNodeDetailedFormatter(activityNode);
    }
    else{
      formatter = new ActivityNodeSimpleFormatter(activityNode);
    }
    registryDebugger.println(formatter.getFormattedText());
    registryDebugger.watchChild(activityNode.getPath() + "/activity-steps", ".*", formatter);
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActiveActivityNodeDebugger: Node = " + activityNode.getPath() + ", TerminateEvent = MODIFY");
    NodeFormatter formatter = null;
    if(this.detailedDebugger){
      formatter = new ActivityNodeDetailedFormatter(activityNode);
    }
    else{
      formatter = new ActivityNodeSimpleFormatter(activityNode);
    }
    registryDebugger.println(formatter.getFormattedText());
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActiveActivityNodeDebugger: Node = " + activityNode.getPath() + ", TerminateEvent = DELETE");
    String path = activityNode.getPath();
    path = path.replace("/active/", "/history/");
    Node historyNode = activityNode.getRegistry().get(path);
    
    NodeFormatter formatter = null;
    if(this.detailedDebugger){
      formatter = new ActivityNodeDetailedFormatter(historyNode);
    }
    else{
      formatter = new ActivityNodeSimpleFormatter(historyNode);
    }
    
    registryDebugger.println(formatter.getFormattedText());
  }

}
