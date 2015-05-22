package com.neverwinterdp.swing.scribengin.dataflow.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.DataflowCommandStartStopResumeTest;
import com.neverwinterdp.scribengin.dataflow.test.DataflowRandomServerFailureTest;
import com.neverwinterdp.scribengin.dataflow.test.HDFSDataflowTest;
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

  static public class DataflowKafkaToKakaTestRunner extends DataflowTestRunner {

    public DataflowKafkaToKakaTestRunner() {
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

  static public class DataflowHDFSToHDFSTestRunner extends DataflowTestRunner {

    public DataflowHDFSToHDFSTestRunner() {
      super("HDFS To HDFS Dataflow Test", "HDFS To HDFS Dataflow Test");
      ScribenginShell shell = Cluster.getCurrentInstance().getScribenginShell();
      String command =
          "dataflow-test " + HDFSDataflowTest.TEST_NAME +
          " --dataflow-name  hdfs-to-hdfs" +
          " --dataflow-id    hdfs-to-hdfs-1" +
          " --worker 3" +
          " --executor-per-worker 1" +
          " --duration 90000" +
          " --task-max-execute-time 1000" +
          " --source-name output" +
          " --source-num-of-stream 10" +
          " --source-write-period 5" +
          " --source-max-records-per-stream 100" +
          " --sink-name output" +
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

  static public class DataflowWorkerFailureTestRunner extends DataflowTestRunner {

    public DataflowWorkerFailureTestRunner() {
      super("Server failure Dataflow Test", "Server failure Dataflow Test");
      ScribenginShell shell = Cluster.getCurrentInstance().getScribenginShell();
      String dataflowWorkerFailureSimulationCommand = 
          "dataflow-test " + DataflowRandomServerFailureTest.TEST_NAME + 
          "  --dataflow-id server-failure-kafka-to-kafka-1" + 
          "  --dataflow-name server-failure-test" + 
          "  --failure-period 10000 --max-failure 5 --simulate-kill";
      add(shell, dataflowWorkerFailureSimulationCommand);

      String dataflowKafkaToKafkaCommand =
          "dataflow-test " + KafkaDataflowTest.TEST_NAME +
          " --dataflow-id    server-failure-kafka-to-kafka-1" +
          " --dataflow-name  server-failure-kafka-to-kafka" +
          " --worker 2 --executor-per-worker 2"+
          " --duration 180000"+
          " --task-max-execute-time 5000" +
          " --source-name input"+
          " --source-num-of-stream 5"+
          " --source-write-period 0"+
          " --source-max-records-per-stream 100000" +
          " --sink-name output " +
          " --debug-dataflow-activity-detail" +
          " --debug-dataflow-task" +
          " --debug-dataflow-vm" +
          " --dump-registry" +
          " --print-dataflow-info -1";
      add(shell, dataflowKafkaToKafkaCommand);
    }
  }

  static public class DataflowStartStopResumtTestRunner extends DataflowTestRunner {

    public DataflowStartStopResumtTestRunner() {
      super("Dataflow Start/Stop/Resumt Test", "Dataflow Start/Stop/Resumt Test");

      ScribenginShell shell = Cluster.getCurrentInstance().getScribenginShell();

      String dataflowStartStopResumeCommand = 
          "dataflow-test " + DataflowCommandStartStopResumeTest.TEST_NAME +
          "  --dataflow-id kafka-to-kafka-1" +
          "  --sleep-before-stop 10000 --sleep-before-resume 5000" +
          "  --max-wait-for-stop    10000 " +
          "  --max-wait-for-resume  5000 " +
          "  --max-execution 3" +
          "  --print-summary ";

      add(shell, dataflowStartStopResumeCommand);

      String dataflowKafkaToKafkaCommand =
          "dataflow-test " + KafkaDataflowTest.TEST_NAME +
          " --dataflow-id    server-failure-kafka-to-kafka-1" +
          " --dataflow-name  server-failure-kafka-to-kafka" +
          " --worker 2 --executor-per-worker 2"+
          " --duration 180000"+
          " --task-max-execute-time 5000" +
          " --source-name input"+
          " --source-num-of-stream 5"+
          " --source-write-period 0"+
          " --source-max-records-per-stream 100000" +
          " --sink-name output " +
          " --debug-dataflow-activity-detail" +
          " --debug-dataflow-task" +
          " --debug-dataflow-vm" +
          " --dump-registry" +
          " --print-dataflow-info -1";
      add(shell, dataflowKafkaToKafkaCommand);
    }
  }
}
