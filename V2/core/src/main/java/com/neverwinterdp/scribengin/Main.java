package com.neverwinterdp.scribengin;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.builder.HelloHDFSDataflowBuilder;
import com.neverwinterdp.scribengin.event.ScribenginAssertEventListener;
import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.YarnVMClient;

public class Main {
  @Parameter(names = "--zk-connect", description = "Zookeeper connect string")
  private String zkConnect = "zookeeper:2181";
  
  @Parameter(names = "--hadoop-master", description = "Hadoop master hostname")
  private String hadoopMaster = "hadoop-master";
  
  @Parameter(names = "--vm-master", description = "Start VM Master")
  private boolean vmMaster = false;
  
  @Parameter(names = "--scribengin-master", description = "Start Scribengin Master")
  private boolean scribenginMaster = false;
  
  @Parameter(names = "--hello-dataflow", description = "Start Hello Dataflow")
  private boolean helloDataflow = false;
  
  public void start() throws Exception {
    System.setProperty("HADOOP_USER_NAME", "neverwinterdp"); 
    Registry registry = newRegistry().connect();
    try {
      Map<String, String> yarnProps = new HashMap<String, String>() ;
      //yarnProps.put("yarn.resourcemanager.scheduler.address", "hadoop-master:8030");
      yarnProps.put("yarn.resourcemanager.address", hadoopMaster + ":8032");
      yarnProps.put("fs.defaultFS", "hdfs://" + hadoopMaster +":9000");
      YarnVMClient vmClient = new YarnVMClient(registry, VMConfig.Environment.YARN, yarnProps) ;

      ScribenginClusterBuilder clusterBuilder = new ScribenginClusterBuilder(new VMClusterBuilder(vmClient)) ;
      if(vmMaster) {
        clusterBuilder.startVMMasters();
        Thread.sleep(5000);
      }

      if(scribenginMaster) {
        clusterBuilder.startScribenginMasters();
      }

      if(helloDataflow) {
        String dataDir = "/data" ;
        FileSystem fs = getFileSystem();
        if(fs.exists(new Path(dataDir))) {
          fs.delete(new Path(dataDir), true) ;
        }
        HelloHDFSDataflowBuilder dataflowBuilder = new HelloHDFSDataflowBuilder(clusterBuilder, fs, dataDir);
        dataflowBuilder.setNumOfWorkers(1);
        dataflowBuilder.setNumOfExecutorPerWorker(2);
        dataflowBuilder.createSource(15, 3, 5);
        HDFSUtil.dump(fs, dataDir + "/source");
        ScribenginShell shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());

        ScribenginAssertEventListener sribenginAssert = dataflowBuilder.submit();
        sribenginAssert.waitForEvents(90000);

        Thread.sleep(3000);
        shell.execute("vm list");
        shell.execute("registry dump --path /");
        HDFSUtil.dump(fs, dataDir + "/sink");
        HDFSUtil.dump(fs, dataDir + "/invalid-sink");
      }
    } finally {
      registry.get("/").dump(System.err);
    }
  }
  
  Registry newRegistry() {
    RegistryConfig config = new RegistryConfig();
    config.setConnect(zkConnect);
    config.setDbDomain("/NeverwinterDP");
    config.setRegistryImplementation(RegistryImpl.class.getName());
    return new RegistryImpl(config);
  }
  
  FileSystem getFileSystem() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://" + hadoopMaster + ":9000");
    return FileSystem.get(conf);
  }
  
  static public void main(String[] args) throws Exception {
    Main main = new Main() ;
    new JCommander(main, args) ;
    main.start();
  }
}
