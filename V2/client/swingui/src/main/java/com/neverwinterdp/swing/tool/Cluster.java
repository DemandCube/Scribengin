package com.neverwinterdp.swing.tool;

import java.util.Random;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.vm.client.VMClient;

abstract public class Cluster {
  static private Cluster CURRENT_INSTANCE ;
  
  public void start() throws Exception {
  }
  
  public void shutdown() throws Exception {
  }
  
  public void startDependencySevers() throws Exception {
  }
  
  public void shutdownDependencySevers() throws Exception {
  }
  
  abstract public void startVMMaster() throws Exception ;
  abstract public void shutdownVMMaster() throws Exception ;
  
  abstract public void startScribenginMaster() throws Exception ;
  abstract public void shutdownScribenginMaster() throws Exception ;
  
  
  abstract public ScribenginShell getScribenginShell() ;
  
  abstract public VMClient getVMClient() ;
  
  abstract public Registry getRegistry() ;

  
  public void runKafkaToKafkaDataflow() throws Exception {
    ScribenginShell shell = getScribenginShell() ;
    String command =
        "dataflow-test " + KafkaDataflowTest.TEST_NAME +
        " --dataflow-id    kafka-to-kafka-1" +
        " --dataflow-name  kafka-to-kafka" +
        " --worker 3" +
        " --executor-per-worker 1" +
        " --duration 90000" +
        " --task-max-execute-time 1000" +
        " --source-name input" +
        " --source-num-of-stream 10" +
        " --source-write-period 0" +
        " --source-max-records-per-stream 10000" +
        " --sink-name output " +
        " --print-dataflow-info -1" +
        " --debug-dataflow-task " +
        " --debug-dataflow-vm " +
        " --debug-dataflow-activity " +
        " --junit-report build/junit-report.xml" + 
        " --dump-registry" + 
        " --vm-client-wait-for-result-timeout 60000";
    shell.execute(command);
  }
  
  static public Cluster getCurrentInstance() { return CURRENT_INSTANCE;  }
  
  static public void setCurrentInstance(Cluster instance) { 
    CURRENT_INSTANCE = instance ; 
  }
}
