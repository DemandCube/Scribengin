package com.neverwinterdp.scribengin.dataflow.activity.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;

//debugger.watchChild("/scribengin/dataflows/running/kafka-to-kafka/activities/active",".*", new ActiveActivityNodeDebugger());
public class ActiveActivityNodeDebugger implements NodeDebugger {

  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActiveActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = CREATE");
    
    
    //TODO FOR TUAN
    /**
     * this watchChild() call below is throwing an exception, 
     * because of this ActiveActivityStepNodeDebugger is never getting called
     * 
     * key output from running ActivityNodeDebuggerUnitTest:
     * watch /scribengin/dataflows/running/kafka-to-kafka/activities/active/init-dataflow-0000000000/activity-steps: org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for /NeverwinterDP/scribengin/dataflows/running/kafka-to-kafka/activities/active/init-dataflow-0000000000/activity-steps
      ActivityNodeWatcher: event = DELETE, path = /scribengin/dataflows/running/kafka-to-kafka/activities/active/init-dataflow-0000000000
      com.neverwinterdp.registry.RegistryException: org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for /NeverwinterDP/scribengin/dataflows/running/kafka-to-kafka/activities/active/init-dataflow-0000000000/activity-steps
        at com.neverwinterdp.registry.zk.RegistryImpl.getChildren(RegistryImpl.java:253)
        at com.neverwinterdp.registry.event.RegistryListener$SelectChildNodeWatcher.updateChildrenWatch(RegistryListener.java:202)
        at com.neverwinterdp.registry.event.RegistryListener$SelectChildNodeWatcher.onEvent(RegistryListener.java:197)
        at com.neverwinterdp.registry.event.RegistryListener$PersistentNodeWatcher.onEvent(RegistryListener.java:156)
        at com.neverwinterdp.registry.zk.ZKNodeWatcher.process(ZKNodeWatcher.java:22)
        at org.apache.zookeeper.ClientCnxn$EventThread.processEvent(ClientCnxn.java:522)
        at org.apache.zookeeper.ClientCnxn$EventThread.run(ClientCnxn.java:498)
      Caused by: org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for /NeverwinterDP/scribengin/dataflows/running/kafka-to-kafka/activities/active/init-dataflow-0000000000/activity-steps
        at org.apache.zookeeper.KeeperException.create(KeeperException.java:111)
        at org.apache.zookeeper.KeeperException.create(KeeperException.java:51)
        at org.apache.zookeeper.ZooKeeper.getChildren(ZooKeeper.java:1472)
        at org.apache.zookeeper.ZooKeeper.getChildren(ZooKeeper.java:1500)
        at com.neverwinterdp.registry.zk.RegistryImpl.getChildren(RegistryImpl.java:250)
     */
    System.err.println("WATCHING CHILDREN: "+activityNode.getPath()+"/activity-steps");
    registryDebugger.watchChild(activityNode.getPath()+"/activity-steps", ".*", new ActiveActivityStepNodeDebugger());
    
    
    ActiveActivityRegistryFormatter formatter = new ActiveActivityRegistryFormatter(activityNode.getParentNode());
    registryDebugger.println(formatter.getFormattedText());
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActiveActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = MODIFY");
    
    ActiveActivityRegistryFormatter formatter = new ActiveActivityRegistryFormatter(activityNode.getParentNode());
    registryDebugger.println(formatter.getFormattedText());
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node activityNode) throws Exception {
    registryDebugger.println("ActiveActivityNodeDebugger: Node = " + activityNode.getPath() + ", Event = DELETE");
  }

}
