package com.neverwinterdp.scribengin;


import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataProcessor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.hdfs.DataGenerator;
import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
import com.neverwinterdp.scribengin.junit.ScribenginAssert;
import com.neverwinterdp.scribengin.service.VMScribenginServiceCommand;
import com.neverwinterdp.scribengin.service.VMScribenginServiceApp;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;

abstract public class VMScribenginUnitTest extends VMUnitTest {
  private SinkFactory sinkFactory;
  private FileSystem fs;
  protected long vmLaunchTime = 100;
  
  @Before
  public void setup() throws Exception {
    super.setup();
    fs = getFileSystem();
    String dataDir = getDataDir() ;
    sinkFactory = new SinkFactory(fs);
    SinkDescriptor sinkDescriptor = new SinkDescriptor("hdfs", dataDir + "/source");
    Sink sink = sinkFactory.create(sinkDescriptor);;
    for(int i = 0; i < 15; i++) {
      DataGenerator.generateNewStream(sink, 3, 5);
    }
    HDFSUtil.dump(fs, getDataDir() + "/source");
  }
  
  @After
  public void teardown() throws Exception {
    super.teardown();
  }
  
  @Test
  public void testMaster() throws Exception {
    ScribenginShell shell = new ScribenginShell(newRegistry().connect());
    VMClient vmClient = shell.getVMClient();
    
    banner("Create VM Master 1");
    ScribenginAssert sribenginAssert = new ScribenginAssert(shell.getScribenginClient().getRegistry());
    sribenginAssert.assertVMStatus("Expect vm-master-1 with running status", "vm-master-1", VMStatus.RUNNING);
    sribenginAssert.assertHeartbeat("Expect vm-master-1 has connected heartbeat", "vm-master-1", true);
    createVMMaster("vm-master-1");
    sribenginAssert.waitForEvents(10000);
    Thread.sleep(vmLaunchTime); //wait to make sure service on the vm running. Need to fix
    
    shell.execute("registry dump --path /vm");
    
    banner("Create Scribengin Master 1 and 2");
    sribenginAssert.assertScribenginMaster("Expect vm-scribengin-master-1 as the leader", "vm-scribengin-master-1");
    VMDescriptor scribenginMaster1 = createVMScribenginMaster(vmClient, "vm-scribengin-master-1") ;
    VMDescriptor scribenginMaster2 = createVMScribenginMaster(vmClient, "vm-scribengin-master-2") ;
    sribenginAssert.waitForEvents(30000);
    
    sribenginAssert.watchDataflow("test-dataflow");
    sribenginAssert.assertDataflowMaster("Expect test-dataflow-master-1 as the leader", "test-dataflow-master-1");
    sribenginAssert.assertDataflowStatus("Expect dataflow init status", "test-dataflow", DataflowLifecycleStatus.INIT);
    sribenginAssert.assertDataflowStatus("Expect dataflow running status", "test-dataflow", DataflowLifecycleStatus.RUNNING);
    sribenginAssert.assertDataflowStatus("Expect dataflow  finish status", "test-dataflow", DataflowLifecycleStatus.FINISH);
    VMDescriptor scribenginMaster = shell.getScribenginClient().getScribenginMaster();
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("test-dataflow");
    dflDescriptor.setNumberOfWorkers(3);
    dflDescriptor.setNumberOfExecutorsPerWorker(3);
    dflDescriptor.setDataProcessor(TestCopyDataProcessor.class.getName());
    SourceDescriptor sourceDescriptor = new SourceDescriptor("HDFS", getDataDir() + "/source") ;
    dflDescriptor.setSourceDescriptor(sourceDescriptor);
    SinkDescriptor defaultSink = new SinkDescriptor("HDFS", getDataDir() + "/sink");
    dflDescriptor.addSinkDescriptor("default", defaultSink);
    SinkDescriptor invalidSink = new SinkDescriptor("HDFS", getDataDir() + "/invalid-sink");
    dflDescriptor.addSinkDescriptor("invalid", invalidSink);
    
    Command deployCmd = new VMScribenginServiceCommand.DataflowDeployCommand(dflDescriptor) ;
    CommandResult<Boolean> result = 
        (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd, 35000);
    Assert.assertTrue(result.getResult());
    sribenginAssert.waitForEvents(60000);
    
    Thread.sleep(3000);
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
  
  abstract protected String getDataDir() ;
  
  abstract protected FileSystem getFileSystem() throws Exception ;
  
  abstract protected void createVMMaster(String name) throws Exception ;
  
  protected VMDescriptor createVMScribenginMaster(VMClient vmClient, String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setName(name).
      addRoles("scribengin-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMScribenginServiceApp.class.getName());
    configureEnvironment(vmConfig);
    System.out.println("VMConfig:");
    System.out.println(JSONSerializer.INSTANCE.toString(vmConfig));
    VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
    Assert.assertNotNull(vmDescriptor);
    return vmDescriptor;
  }
  
  abstract protected void configureEnvironment(VMConfig vmConfig) ;
  
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