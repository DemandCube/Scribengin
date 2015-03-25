package com.neverwinterdp.scribengin;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.YarnVMClient;

public class ShellMain {
  static public void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "neverwinterdp"); 
    if(args.length > 0 && args[0].equals("help")){
      Registry registry = new RegistryImpl();
      VMClient vmClient = new VMClient(registry);
      ScribenginShell shell = new ScribenginShell(vmClient) ;
      shell.execute(args);
      return;
    }
    String zkConnect = System.getProperty("shell.zk-connect");
    RegistryConfig registryConfig = RegistryConfig.getDefault();
    registryConfig.setConnect(zkConnect);
    Registry registry = null;
    try{
      registry = new RegistryImpl(registryConfig).connect();
    } catch(Exception e){
      System.err.println("Could not connect to the registry at: "+ registryConfig.getConnect()+"\n"+e.getMessage());
      return;
    }
    
    String hadoopMaster = System.getProperty("shell.hadoop-master");
    HadoopProperties hadoopProps = new HadoopProperties() ;
    hadoopProps.put("yarn.resourcemanager.address", hadoopMaster + ":8032");
    hadoopProps.put("fs.defaultFS", "hdfs://" + hadoopMaster +":9000");
    
    YarnVMClient vmClient = new YarnVMClient(registry, VMConfig.Environment.YARN, hadoopProps) ;
    ScribenginShell shell = new ScribenginShell(vmClient) ;
    shell.attribute(HadoopProperties.class, hadoopProps);
    shell.execute(args);
  }
}