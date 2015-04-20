package com.neverwinterdp.scribengin.dataflow.test;

import java.util.List;
import java.util.Random;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowRandomServerFailureTest extends DataflowCommandTest {
  final static public String TEST_NAME = "random-server-failure";

  @Parameter(names = "--flow-name", description = "The command should repeat in this period of time")
  String flowName = "kafka-to-kafka";
  
  @Parameter(names = "--print-summary", description = "Enable to dump the registry at the end")
  protected boolean printSummary = false;
  
  public void doRun(ScribenginShell shell) throws Exception {
    ScribenginClient scribenginClient = shell.getScribenginClient() ;
    DataflowClient dflClient = scribenginClient.getDataflowClient(flowName);
    doKillRandomWorker(dflClient);
  }
  
  ExecuteLog doKillRandomWorker(DataflowClient dflClient) throws Exception {
    ExecuteLog executeLog = new ExecuteLog("Randomly kill a dataflow worker") ;
    VMDescriptor selWorker = this.selectRandom(dflClient.getDataflowWorkers());
    dflClient.getScribenginClient().getVMClient().shutdown(selWorker);
    return executeLog;
  }
  
  ExecuteLog doShutdownWorker(DataflowClient dflClient) throws Exception {
    ExecuteLog executeLog = new ExecuteLog("Stop the dataflow with the event") ;
    return executeLog;
  }
  
  ExecuteLog doKillMaster(DataflowClient dflClient) throws Exception {
    ExecuteLog executeLog = new ExecuteLog("Stop the dataflow with the event") ;
    return executeLog;
  }
  
  ExecuteLog doShutdownMaster(DataflowClient dflClient) throws Exception {
    ExecuteLog executeLog = new ExecuteLog("Stop the dataflow with the event") ;
    return executeLog;
  }
  
  VMDescriptor selectRandom(List<VMDescriptor> vmDescriptors) throws Exception {
    if(vmDescriptors.size() < 1) {
      throw new Exception("") ;
    }
    Random rand = new Random() ;
    int selIndex = rand.nextInt(vmDescriptors.size()) ;
    return vmDescriptors.get(selIndex) ;
  }
}