package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;

abstract public class DataflowTest {
  @Parameter(names = "--flow-name", description = "The flow name")
  protected String name = "hello";
  
  @Parameter(names = "--worker", description = "Number of the workers")
  protected int    numOfWorkers           = 3;
  
  @Parameter(names = "--executor-per-worker", description = "The number of executor per worker")
  protected int    numOfExecutorPerWorker = 3;
  
  @Parameter(names = "--task-max-execute-time", description = "The max time an executor should work on a task")
  protected long   taskMaxExecuteTime = 10000;
  
  @Parameter(names = "--duration", description = "Max duration for the test")
  protected long duration = 60000;
  
  @Parameter(names = "--print-dataflow-info", description = "Max duration for the test")
  protected long printDataflowInfo = 5000;
  
  public void run(ScribenginShell shell) throws Exception {
    doRun(shell);
  }
  
  protected Thread newPrintDataflowThread(ScribenginShell shell, DataflowDescriptor descriptor) throws Exception {
    return new PrintDataflowInfoThread(shell, descriptor, printDataflowInfo) ;
  }
  
  abstract protected void doRun(ScribenginShell shell) throws Exception ;

  static public class PrintDataflowInfoThread extends Thread {
    ScribenginShell shell;
    DataflowDescriptor descriptor;
    long period ;
    
    PrintDataflowInfoThread(ScribenginShell shell, DataflowDescriptor descriptor, long period) {
      this.shell = shell ;
      this.descriptor = descriptor;
      this.period = period;
    }
    
    public void run() {
      try {
        while(true) {
          Thread.sleep(period);
          try {
            shell.execute("dataflow info --running " + descriptor.getName());
          } catch(Exception ex) {
            System.err.println(ex.getMessage());
          }
        }
      } catch(InterruptedException ex) {
      }
    }
  }
}
