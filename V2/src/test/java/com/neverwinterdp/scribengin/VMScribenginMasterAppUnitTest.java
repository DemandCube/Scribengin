package com.neverwinterdp.scribengin;


import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.CopyDataProcessor;
import com.neverwinterdp.scribengin.dataflow.DataProcessor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.hdfs.DataGenerator;
import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.source.SourceFactory;
import com.neverwinterdp.scribengin.vm.VMScribenginCommand;
import com.neverwinterdp.scribengin.vm.VMScribenginMasterApp;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMServicePlugin;
import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.jvm.JVMVMServicePlugin;
import com.neverwinterdp.vm.master.VMManagerApp;

public class VMScribenginMasterAppUnitTest extends VMUnitTest {
  static String      SOURCE_DIRECTORY       = "./build/hdfs/source";
  static String      SINK_DIRECTORY         = "./build/hdfs/sink";
  static String      INVALID_SINK_DIRECTORY = "./build/hdfs/invalid-sink";

  private SinkFactory sinkFactory;
  private FileSystem fs;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/hdfs", false);
    super.setup();
    fs = FileSystem.get(new Configuration());
    sinkFactory = new SinkFactory(fs);
    SinkDescriptor sinkDescriptor = new SinkDescriptor("hdfs", SOURCE_DIRECTORY);
    Sink sink = sinkFactory.create(sinkDescriptor);;
    for(int i = 0; i < 15; i++) {
      DataGenerator.generateNewStream(sink, 3, 5);
    }
  }
  
  @After
  public void teardown() throws Exception {
    super.teardown();
  }
  
  @Test
  public void testMaster() throws Exception {
    banner("Create VM Master 1");
    VM vmMaster1 = createVMMaster("vm-master-1");
    Thread.sleep(500);
   
    Thread.sleep(1000);
    ScribenginShell shell = new ScribenginShell(newRegistry().connect());
    VMClient vmClient = shell.getVMClient();
    shell.execute("vm list");
    shell.execute("registry dump");

    banner("Create Scribengin Master");
    VMDescriptor scribenginMaster1 = createVMScribenginMaster(vmClient, "vm-scribengin-master-1") ;
    Thread.sleep(2000);
    shell.execute("vm list");
    shell.execute("registry dump --path /");
    
    banner("Create Scribengin Master");
    VMDescriptor scribenginMaster2 = createVMScribenginMaster(vmClient, "vm-scribengin-master-2") ;
    Thread.sleep(2000);
    
    VMDescriptor scribenginMaster = shell.getScribenginClient().getScribenginMaster();
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("test-dataflow");
    dflDescriptor.setNumberOfWorkers(3);
    dflDescriptor.setNumberOfExecutorsPerWorker(3);
    dflDescriptor.setDataProcessor(TestCopyDataProcessor.class.getName());
    SourceDescriptor sourceDescriptor = new SourceDescriptor("HDFS", SOURCE_DIRECTORY) ;
    dflDescriptor.setSourceDescriptor(sourceDescriptor);
    SinkDescriptor defaultSink = new SinkDescriptor("HDFS", SINK_DIRECTORY);
    dflDescriptor.addSinkDescriptor("default", defaultSink);
    SinkDescriptor invalidSink = new SinkDescriptor("HDFS", INVALID_SINK_DIRECTORY);
    dflDescriptor.addSinkDescriptor("invalid", invalidSink);
    
    Command deployCmd = new VMScribenginCommand.DataflowDeployCommand(dflDescriptor) ;
    CommandResult<Boolean> result = 
        (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd, 35000);
    Assert.assertTrue(result.getResult());
    
    Thread.sleep(5000);
    shell.execute("vm list");
    shell.execute("registry dump --path /");
    HDFSUtil.dump(fs, SINK_DIRECTORY);
    HDFSUtil.dump(fs, INVALID_SINK_DIRECTORY);
  }

  private void banner(String title) {
    System.out.println("\n\n");
    System.out.println("------------------------------------------------------------------------");
    System.out.println(title);
    System.out.println("------------------------------------------------------------------------");
  }
  
  private VM createVMMaster(String name) throws Exception {
    String[] args = {
      "--name", name,
      "--roles", "vm-master",
      "--self-registration",
      "--registry-connect", "127.0.0.1:2181", 
      "--registry-db-domain", "/NeverwinterDP", 
      "--registry-implementation", RegistryImpl.class.getName(),
      "--vm-application",VMManagerApp.class.getName(),
      "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + JVMVMServicePlugin.class.getName()
    };
    VMConfig vmConfig = new VMConfig();
    vmConfig.
      setName("vm-master").
      setSelfRegistration(true);
    VM vm = VM.run(args);
    return vm;
  }
  
  private VMDescriptor createVMScribenginMaster(VMClient vmClient, String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setName(name).
      addRoles("scribengin-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMScribenginMasterApp.class.getName());
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
}