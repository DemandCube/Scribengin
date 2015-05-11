package com.neverwinterdp.swing.tool;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;

public class Cluster {
  static private Cluster INSTANCE = new Cluster() ;
  
  
  private ScribenginClusterBuilder clusterBuilder;
  private ClusterConfig            config = new ClusterConfig();
  private ScribenginShell          shell;

  public ClusterConfig getClusterConfig() { return this.config ; }
  
  public ScribenginClusterBuilder getClusterBuilder() {
    return clusterBuilder;
  }

  public ClusterConfig getConfig() {
    return config;
  }

  public VMClient getVMClient() {
    if(clusterBuilder != null) {
      return clusterBuilder.getVMClusterBuilder().getVMClient();
    }
    return null ;
  }
  
  public Registry getRegistry() {
    if(clusterBuilder != null) {
      return clusterBuilder.getVMClusterBuilder().getVMClient().getRegistry();
    }
    return null ;
  }
  
  public ScribenginShell getScribenginShell() { return shell;}

  public void launch() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder());
    clusterBuilder.clean();
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();

    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
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
        " --dump-registry";
    shell.execute(command);
  }

  public void shutdown() throws Exception {
    clusterBuilder.shutdown();
    clusterBuilder = null ;
    shell = null ;
  }

  static public Cluster getInstance() { return INSTANCE; }
}
