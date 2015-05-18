package com.neverwinterdp.scribengin.dataflow.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowRandomServerFailureTest extends DataflowCommandTest {
  final static public String TEST_NAME = "random-server-failure";

  @Parameter(names = "--dataflow-id", required=true, description = "The command should repeat in this period of time")
  String dataflowId = "DataflowRandomServerFailureTest-unknown";
  
  @Parameter(names = "--wait-for-running-dataflow", description = "The command should repeat in this failurePeriod of time")
  long waitForRunningDataflow = 180000;
  
  @Parameter(names = "--wait-before-simulate-failure", description = "Wait before simulate")
  long waitBeforeSimulateFailure = 15000;
  
  
  @Parameter(names = "--failure-period", description = "The command should repeat in this period of time")
  long failurePeriod = 15000;
  
  @Parameter(names = "--max-failure", description = "The command should repeat in this period of time")
  int maxFailure = 100;
  
  @Parameter(names = "--simulate-kill", description = "The command should repeat in this period of time")
  boolean simulateKill = false;
  
  @Parameter(names = "--print-summary", description = "Enable to dump the registry at the end")
  protected boolean printSummary = false;
  
  public void doRun(ScribenginShell shell) throws Exception {

    try {
      ScribenginClient scribenginClient = shell.getScribenginClient() ;
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId, waitForRunningDataflow);
      Registry registry = dflClient.getRegistry();
      long stopTime = System.currentTimeMillis() + waitForRunningDataflow;
      while(dflClient.countActiveDataflowWorkers() == 0 && System.currentTimeMillis() < stopTime) {
        Thread.sleep(500);
      }
      
      if(waitBeforeSimulateFailure > 0) {
        Thread.sleep(waitBeforeSimulateFailure);
      }
      List<ExecuteLog> executeLogs = new ArrayList<ExecuteLog>() ;
      boolean error = false ;
      int simulationCount = 0 ;
      FailureSimulator[] failureSimulator = {
          new RandomWorkerKillFailureSimulator()
      } ;
      
      while(!error && simulationCount < maxFailure && dflClient.getStatus() == DataflowLifecycleStatus.RUNNING) {
        Notifier notifier = 
          new Notifier(registry, "/scribengin/tests/dataflow-random-server-kill/notification", "simulation-" + ((simulationCount + 1)));
        ExecuteLog executeLog = failureSimulator[0].terminate(dflClient, notifier);
        if(executeLog != null) {
          executeLogs.add(executeLog);
          Thread.sleep(failurePeriod);
        } else {
          error = true ;
        }
        simulationCount++ ;
      }
      report(shell, executeLogs);
    } catch(Exception ex) {
      ex.printStackTrace();
      shell.execute("registry dump");
      shell.execute("dataflow info --dataflow-id " + dataflowId);
      throw ex ;
    }
  }

  VMDescriptor selectRandomVM(List<VMDescriptor> vmDescriptors, Notifier logger) throws Exception {
    if(vmDescriptors.size() == 0) {
      logger.info("select-random-server", "No server to select");
      return null ;
    }
    Random rand = new Random() ;
    int selIndex = rand.nextInt(vmDescriptors.size()) ;
    VMDescriptor selectedVM =  vmDescriptors.get(selIndex) ;
    StringBuilder vmList = new StringBuilder() ;
    for(VMDescriptor sel : vmDescriptors) {
      if(vmList.length() > 0) vmList.append(", ");
      vmList.append(sel.getId());
    }
    logger.info("select-random-server", "Select " + selectedVM.getId() + " from " + vmList);
    return selectedVM;
  }
  
  abstract public class FailureSimulator {
    abstract public ExecuteLog terminate(DataflowClient dflClient, Notifier logger) throws Exception ;
  }
  
  public class RandomWorkerKillFailureSimulator extends FailureSimulator {
    public ExecuteLog terminate(DataflowClient dflClient, Notifier logger) throws Exception {
      ExecuteLog executeLog = new ExecuteLog() ;
      executeLog.start();
      try {
        VMDescriptor selWorker = selectRandomVM(dflClient.getActiveDataflowWorkers(), logger);
        if(selWorker != null) {
          executeLog.setDescription("Kill the dataflow worker " + selWorker.getId());
          if(simulateKill) {
            logger.info("before-simulate-kill", "Before simulate kill " + selWorker.getId());
            dflClient.getScribenginClient().getVMClient().simulateKill(selWorker);
            logger.info("after-simulate-kill", "After simulate kill " + selWorker.getId());
          } else {
            logger.info("before-kill", "Before kill " + selWorker.getId());
            dflClient.getScribenginClient().getVMClient().kill(selWorker);
            logger.info("after-kill", "After kill " + selWorker.getId());
          }
        } else {
          executeLog.setDescription("No available worker is found to kill");
        }
      } finally {
        executeLog.stop();
        logger.info("kill-result", executeLog.getFormatText());
      }
      return executeLog;
    }
  }
}