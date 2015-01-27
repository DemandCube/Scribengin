package com.neverwinterdp.scribengin;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.builder.HelloHDFSDataflowBuilder;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.YarnVMClient;

public class HelloScribengin {
  static Registry newRegistry() {
    RegistryConfig config = new RegistryConfig();
    config.setConnect("zookeeper:2181");
    config.setDbDomain("/NeverwinterDP");
    config.setRegistryImplementation(RegistryImpl.class.getName());
    return new RegistryImpl(config);
  }
  
  static FileSystem getFileSystem() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
    return FileSystem.get(conf);
  }
  
  
  static public void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "neverwinterdp"); 
    
    Registry registry = newRegistry();
    try {
      Map<String, String> yarnProps = new HashMap<String, String>() ;
      //yarnProps.put("yarn.resourcemanager.scheduler.address", "hadoop-master:8030");
      yarnProps.put("yarn.resourcemanager.address", "hadoop-master:8032");
      yarnProps.put("fs.defaultFS", "hdfs://hadoop-master:9000");
      YarnVMClient vmClient = new YarnVMClient(registry, VMConfig.Environment.YARN, yarnProps) ;

      ScribenginClusterBuilder clusterBuilder = new ScribenginClusterBuilder(new VMClusterBuilder(vmClient)) ;
      clusterBuilder.start();

      String dataDir = "/data" ;
      FileSystem fs = getFileSystem();
      HelloHDFSDataflowBuilder dataflowBuilder = new HelloHDFSDataflowBuilder(clusterBuilder, fs, dataDir);
      dataflowBuilder.setNumOfWorkers(1);
      dataflowBuilder.setNumOfExecutorPerWorker(2);
      dataflowBuilder.createSource(15, 3, 5);
      HDFSUtil.dump(fs, dataDir + "/source");
      ScribenginShell shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());

      ScribenginWaitingEventListener sribenginAssert = dataflowBuilder.submit();
      sribenginAssert.waitForEvents(90000);

      Thread.sleep(3000);
      shell.execute("vm list");
      shell.execute("registry dump --path /");
      HDFSUtil.dump(fs, dataDir + "/sink");
      HDFSUtil.dump(fs, dataDir + "/invalid-sink");
    } catch(Exception ex) {
      registry.get("/").dump(System.err);
      ex.printStackTrace();
    }
  }
}
