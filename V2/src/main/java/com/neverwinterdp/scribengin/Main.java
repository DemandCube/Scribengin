package com.neverwinterdp.scribengin;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataProcessor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.hdfs.DataGenerator;
import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.vm.VMScribenginCommand;
import com.neverwinterdp.scribengin.vm.VMScribenginMasterApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMServicePlugin;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.master.VMManagerApp;
import com.neverwinterdp.vm.yarn.AppClient;
import com.neverwinterdp.vm.yarn.YarnVMServicePlugin;

public class Main {
  private long vmLaunchTime = 30 * 1000; //30s
  
  private FileSystem fs;
  private ScribenginShell shell;
  private VMClient vmClient;
  
  public void setup() throws Exception {
    fs = getFileSystem();
    String dataDir = getDataDir() ;
    fs.delete(new Path(dataDir), true);
    
    SinkFactory sinkFactory = new SinkFactory(fs);
    SinkDescriptor sinkDescriptor = new SinkDescriptor("hdfs", dataDir + "/source");
    Sink sink = sinkFactory.create(sinkDescriptor);;
    for(int i = 0; i < 15; i++) {
      DataGenerator.generateNewStream(sink, 3, 5);
    }
    HDFSUtil.dump(fs, getDataDir() + "/source");
    
    Registry registry = newRegistry().connect();
    shell = new ScribenginShell(registry);
    vmClient = shell.getVMClient();
    shell.execute("registry dump --path /");
  }
  
  void createVMMaster() throws Exception {
    System.setProperty("HADOOP_USER_NAME", "neverwinterdp"); 
    String[] runArgs = {
      //"--local-home", "build/release/Scribengin.V2",
      "--environment", "YARN",
      "--local-home", ".",
      "--dfs-home", "/apps/scribengin.v2",
      "--name", "VMMaster",
      "--roles", "vm-master",
      "--self-registration",
      "--registry-connect", "zookeeper:2181", 
      "--registry-db-domain", "/NeverwinterDP", 
      "--registry-implementation", RegistryImpl.class.getName(),
      "--vm-application",VMManagerApp.class.getName(),
      "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + YarnVMServicePlugin.class.getName(),
      "--yarn:yarn.resourcemanager.scheduler.address=hadoop-master:8030",
      "--yarn:yarn.resourcemanager.address=hadoop-master:8032",
      "--yarn:fs.defaultFS=hdfs://hadoop-master:9000",
      //"--yarn:fs.default.name=hdfs://hadoop-master:9000",
    };
    AppClient appClient = new AppClient() ;
    appClient.run(runArgs, new YarnConfiguration());
  }
  
  Registry newRegistry() {
    RegistryConfig config = new RegistryConfig();
    config.setConnect("zookeeper:2181");
    config.setDbDomain("/NeverwinterDP");
    config.setRegistryImplementation(RegistryImpl.class.getName());
    return new RegistryImpl(config);
  }
 
  
  public void run() throws Exception {
    banner("Setup");
    setup();
    
    banner("Create VM Master 1");
    createVMMaster();
   
    Thread.sleep(vmLaunchTime);
    shell.execute("registry dump --path /");
    shell.execute("vm list");
    
    banner("Create Scribengin Master 1");
    VMDescriptor scribenginMaster1 = createVMScribenginMaster(vmClient, "vm-scribengin-master-1") ;
    Thread.sleep(vmLaunchTime);
    shell.execute("vm list");
    shell.execute("registry dump --path /");
    
    //banner("Create Scribengin Master 2");
    //VMDescriptor scribenginMaster2 = createVMScribenginMaster(vmClient, "vm-scribengin-master-2") ;
    //Thread.sleep(vmLaunchTime);
    
    VMDescriptor scribenginMaster = shell.getScribenginClient().getScribenginMaster();
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("test-dataflow");
    dflDescriptor.setNumberOfWorkers(1);
    dflDescriptor.setNumberOfExecutorsPerWorker(1);
    dflDescriptor.setDataProcessor(TestCopyDataProcessor.class.getName());
    SourceDescriptor sourceDescriptor = new SourceDescriptor("HDFS", getDataDir() + "/source") ;
    dflDescriptor.setSourceDescriptor(sourceDescriptor);
    SinkDescriptor defaultSink = new SinkDescriptor("HDFS", getDataDir() + "/sink");
    dflDescriptor.addSinkDescriptor("default", defaultSink);
    SinkDescriptor invalidSink = new SinkDescriptor("HDFS", getDataDir() + "/invalid-sink");
    dflDescriptor.addSinkDescriptor("invalid", invalidSink);
    
    Command deployCmd = new VMScribenginCommand.DataflowDeployCommand(dflDescriptor) ;
    CommandResult<Boolean> result = 
        (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd, 35000);
    Assert.assertTrue(result.getResult());
    
    Thread.sleep(vmLaunchTime * 3);
    
    shell.execute("vm list");
    shell.execute("registry dump --path /");
    HDFSUtil.dump(fs, getDataDir() + "/sink");
    HDFSUtil.dump(fs, getDataDir() + "/invalid-sink");
  }

  private void banner(String title) {
    System.out.println("\n\n");
    System.out.println("------------------------------------------------------------------------");
    System.out.println(title);
    System.out.println("------------------------------------------------------------------------");
  }
  
  String getDataDir() { return "/data" ; }
  
  FileSystem getFileSystem() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
    return FileSystem.get(conf);
  }
  
  protected VMDescriptor createVMScribenginMaster(VMClient vmClient, String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setName(name).
      addRoles("scribengin-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMScribenginMasterApp.class.getName());
    vmConfig.setEnvironment(VMConfig.Environment.YARN);
    vmConfig.addYarnProperty("yarn.resourcemanager.scheduler.address", "localhost:8030");
    vmConfig.addYarnProperty("fs.defaultFS", "hdfs://hadoop-master:9000");
    VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
    Assert.assertNotNull(vmDescriptor);
    return vmDescriptor;
  }
  
  static public class TestCopyDataProcessor implements DataProcessor {
    private int count = 0;
    private Random random = new Random();
    
    @Override
    public void process(Record record, DataflowTaskContext ctx) throws Exception {
      if(random.nextDouble() < 0.8) {
        ctx.write(record);
        //System.out.println("Write default");
      } else {
        ctx.write("invalid", record);
        //System.out.println("Write invalid");
      }
      count++ ;
      if(count == 100) {
        ctx.commit();
        count = 0;
      }
    }
  }
  
  static public void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "neverwinterdp"); 
    new Main().run();
  }
}
