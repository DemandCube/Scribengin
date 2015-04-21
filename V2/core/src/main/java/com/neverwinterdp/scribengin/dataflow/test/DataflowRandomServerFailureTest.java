package com.neverwinterdp.scribengin.dataflow.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowRandomServerFailureTest extends DataflowCommandTest {
  final static public String TEST_NAME = "random-server-failure";

  @Parameter(names = "--dataflow-name", description = "The command should repeat in this period of time")
  String dataflowName = "kafka-to-kafka";
  
  @Parameter(names = "--wait-for-running-dataflow", description = "The command should repeat in this failurePeriod of time")
  long waitForRunningDataflow = 180000;
  
  @Parameter(names = "--failure-period", description = "The command should repeat in this period of time")
  long failurePeriod = 5000;
  
  @Parameter(names = "--max-failure", description = "The command should repeat in this period of time")
  int maxFailure = 100;
  
  @Parameter(names = "--simulate-kill", description = "The command should repeat in this period of time")
  boolean simulateKill = false;
  
  @Parameter(names = "--print-summary", description = "Enable to dump the registry at the end")
  protected boolean printSummary = false;
  
  public void doRun(ScribenginShell shell) throws Exception {
    ScribenginClient scribenginClient = shell.getScribenginClient() ;
    DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowName);
    dflClient.waitForDataflowStatus(waitForRunningDataflow, DataflowLifecycleStatus.RUNNING);

    List<ExecuteLog> executeLogs = new ArrayList<ExecuteLog>() ;
    boolean error = false ;
    int failureCount = 0 ;
    FailureSimulator[] failureSimulator = {
      new RandomWorkerKillFailureSimulator()
    } ;
    while(!error && failureCount < maxFailure && dflClient.getStatus() == DataflowLifecycleStatus.RUNNING) {
      ExecuteLog executeLog = failureSimulator[0].terminate(dflClient);
      if(executeLog != null) {
        executeLogs.add(executeLog);
        Thread.sleep(failurePeriod);
      } else {
        error = true ;
      }
      failureCount++ ;
    }
    report(shell, executeLogs);
  }
  
  abstract public class FailureSimulator {
    abstract public ExecuteLog terminate(DataflowClient dflClient) throws Exception ;
    
    VMDescriptor selectRandomVM(List<VMDescriptor> vmDescriptors) throws Exception {
      if(vmDescriptors.size() == 0) return null ;
      Random rand = new Random() ;
      int selIndex = rand.nextInt(vmDescriptors.size()) ;
      return vmDescriptors.get(selIndex) ;
    }
  }
  
  public class RandomWorkerKillFailureSimulator extends FailureSimulator {
    public ExecuteLog terminate(DataflowClient dflClient) throws Exception {
      ExecuteLog executeLog = new ExecuteLog("Kill random a dataflow worker") ;
      executeLog.start();
      try {
        VMDescriptor selWorker = selectRandomVM(dflClient.getDataflowWorkers());
        if(selWorker == null) return null ;
        if(simulateKill) {
          System.err.println("RandomWorkerKillFailureSimulator: simulateKill");
          dflClient.getScribenginClient().getVMClient().simulateKill(selWorker);
        } else {
          System.err.println("RandomWorkerKillFailureSimulator: kill");
          dflClient.getScribenginClient().getVMClient().kill(selWorker);
        }
      } finally {
        executeLog.stop();
      }
      return executeLog;
    }
  }
}