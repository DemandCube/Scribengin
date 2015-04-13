package com.neverwinterdp.scribengin.dataflow.util;

import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.vm.util.VMRegistryFormatter;


/**
 * TODO: Here is how the dataflow vm hierachy look like:
 *  workers
      history
      active
        kafka-to-kafka-worker-1 - {"path":"/vm/allocated/kafka-to-kafka-worker-1"}
          executors
            executor-0 - {"id":"executor-0","status":"RUNNING","assignedTaskIds":[0,3,5,7,9,0,4,6,7,9]
        kafka-to-kafka-worker-2 - {"path":"/vm/allocated/kafka-to-kafka-worker-2"}
          executors
            executor-0 - {"id":"executor-0","status":"TERMINATED","assignedTaskIds":[1]}
        kafka-to-kafka-worker-3 - {"path":"/vm/allocated/kafka-to-kafka-worker-3"}
          executors
            executor-0 - {"id":"executor-0","status":"TERMINATED","assignedTaskIds":[2,4,6,8,1,2,3,5,8
    status - "FINISH"
    master
      leader - {"path":"/vm/allocated/kafka-to-kafka-master-1"}
 * 
 * 1. rename DataflowWorkerNodeDebugger to DataflowVMDebugger to reflect the role of the class
 * 2. Create a formater that print out the information of the dataflow master and dataflow worker
 * 3. Any vm is added to workers/active or removed, use formater(2) to print out the information of all vm
 * 4. Any change in master/leader use formater(2) to print out the information of all vm
 * 
 * Note that we do not need to watch the real vm status or reuse the VMNodeDebugger.
 *
 */
public class DataflowWorkerNodeDebugger implements NodeDebugger{
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node node) throws Exception {
    //Grabs the path from the node passed in
    //Adds a watch for the node in that path using VMNodeDebugger
    registryDebugger.println("DataflowWorkerNodeDebugger: Node = " + node.getPath() + ", Event = CREATE");
    Map<?,?> nodeData = node.getDataAs(Map.class);
    
    Node vmNode = new Node(registryDebugger.getRegistry(), (String)nodeData.get("path"));
    VMRegistryFormatter vmformatter = new VMRegistryFormatter(vmNode);
    
    registryDebugger.watch((String)nodeData.get("path"), vmformatter, true);
    
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
}
