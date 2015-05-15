package com.neverwinterdp.swing.scribengin.dataflow.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.swing.tool.Cluster;

public class DataflowTestRunner extends Thread {
  private String label;
  private String description ;
  private List<ShellCommandRunner> shellCommandRunners = new ArrayList<>();
  
  public DataflowTestRunner(String label, String desc) {
    this.label = label; 
    this.description = desc;
  }
  
  public String getLabel() { return this.label ; }
  
  public String getDescription() { return this.description ; }
  
  public void add(ScribenginShell shell, String command) {
    shellCommandRunners.add(new ShellCommandRunner(shell, command)) ;
  }
  
  public void run() {
    ExecutorService service =  Executors.newFixedThreadPool(shellCommandRunners.size());
    for(int i = 0; i < shellCommandRunners.size(); i++) {
      ShellCommandRunner sel = shellCommandRunners.get(i);
      service.submit(sel);
    }
    service.shutdown();
    try {
      service.awaitTermination(10, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  static public class ShellCommandRunner implements Runnable {
    private ScribenginShell shell ;
    private String          command ;
    
    ShellCommandRunner(ScribenginShell shell, String command) {
      this.shell   = shell ;
      this.command = command ;
    }
    
    public void run() {
      try {
        shell.execute(command);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  static public class KafkaToKakaDataflowTestRunner extends DataflowTestRunner {
    
    public KafkaToKakaDataflowTestRunner() {
      super("Kafka To Kafka Dataflow Test", "Kafka To Kafka Dataflow Test");
      ScribenginShell shell = Cluster.getCurrentInstance().getScribenginShell() ;
      String command =
          "dataflow-test " + KafkaDataflowTest.TEST_NAME +
          " --dataflow-id    kafka-to-kafka-1" +
          " --dataflow-name  kafka-to-kafka" +
          " --worker 3" +
          " --executor-per-worker 1" +
          " --duration 90000" +
          " --task-max-execute-time 1000" +
          " --source-name input" +
          " --source-num-of-stream 10" +
          " --source-write-period 0" +
          " --source-max-records-per-stream 10000" +
          " --sink-name output " +
          " --print-dataflow-info -1" +
          " --debug-dataflow-task " +
          " --debug-dataflow-vm " +
          " --debug-dataflow-activity " +
          " --junit-report build/junit-report.xml" + 
          " --dump-registry" + 
          " --vm-client-wait-for-result-timeout 60000";
      add(shell, command);
    }
  }
}
