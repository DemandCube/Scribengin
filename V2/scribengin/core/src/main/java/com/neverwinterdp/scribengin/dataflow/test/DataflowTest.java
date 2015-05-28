package com.neverwinterdp.scribengin.dataflow.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.tap4j.model.TestResult;
import org.tap4j.model.TestSet;
import org.tap4j.producer.TapProducer;
import org.tap4j.producer.TapProducerFactory;
import org.tap4j.util.StatusValues;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSinkValidatorReport;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSourceGeneratorReport;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.util.text.TabularFormater;

abstract public class DataflowTest {
  @Parameter(names = "--dataflow-id", description = "The dataflow id")
  protected String dataflowId = "hello-1";
  
  @Parameter(names = "--dataflow-name", description = "The flow dataflow name")
  protected String dataflowName = "hello";

  @Parameter(names = "--worker", description = "Number of the workers")
  protected int numOfWorkers = 3;

  @Parameter(names = "--executor-per-worker", description = "The number of executor per worker")
  protected int numOfExecutorPerWorker = 3;

  @Parameter(names = "--task-max-execute-time", description = "The max time an executor should work on a task")
  protected long taskMaxExecuteTime = 10000;

  @Parameter(names = "--duration", description = "Max duration for the test")
  protected long duration = 60000;

  @Parameter(names = "--print-dataflow-info", description = "print data flow info")
  protected long printDataflowInfo = 5000;

  @Parameter(names = "--debug-dataflow-task", description = "Enable the debug dataflow task")
  protected boolean debugDataflowTask = false;
  
  @Parameter(names = "--debug-dataflow-task-detail", description = "Enable detailed debugger")
  protected boolean detailedDebugDataflowTask = false;
  
  @Parameter(names = "--debug-dataflow-vm", description = "Enable the debug dataflow worker")
  protected boolean debugDataflowVM = false;
  
  @Parameter(names = "--debug-dataflow-vm-detail", description = "Enable detailed debugger")
  protected boolean detailedDebugDataflowVM = false;
  
  @Parameter(names = "--debug-dataflow-activity", description = "Enable the debug dataflow worker")
  protected boolean debugDataflowActivity = false;
  
  @Parameter(names = "--debug-dataflow-activity-detail", description = "Enable detailed debugger")
  protected boolean detailedDebugDataflowActivity = false;

  @Parameter(names = "--dump-registry", description = "Enable to dump the registry at the end")
  protected boolean dumpRegistry = false;

  @Parameter(names = "--junit-report", description = "The junit report output file")
  protected String junitReport;

  @Parameter(names = "--vm-client-wait-for-result-timeout", description = "Max duration for the test")
  protected long vmClientWaitForResultTimeout = 60000;
  
  public void run(ScribenginShell shell) throws Exception {
    shell.getVMClient().setWaitForResultTimeout(vmClientWaitForResultTimeout);
    doRun(shell);
  }

  public void sourceToSinkDataflowTest(ScribenginShell shell, DataflowSourceGenerator sourceGenerator, 
                                       DataflowSinkValidator sinkValidator) throws Exception{
    ScribenginClient scribenginClient = shell.getScribenginClient();
    sourceGenerator.init(scribenginClient);
    if(sourceGenerator.canRunInBackground()){
      sourceGenerator.runInBackground();
    } else{
      sourceGenerator.run();
    }
    sinkValidator.init(scribenginClient);
    
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setId(dataflowId);
    dflDescriptor.setName(dataflowName);
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setTaskMaxExecuteTime(taskMaxExecuteTime);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());
    dflDescriptor.setSourceDescriptor(sourceGenerator.getSourceDescriptor());
    dflDescriptor.addSinkDescriptor("default",sinkValidator.getSinkDescriptor());
    
    setupDebugger(shell, scribenginClient, dflDescriptor);
   
    DataflowWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
    
    try {
      waitingEventListener.waitForEvents(duration);
    } catch (Exception e) {
    }
    report(shell, waitingEventListener);

    //TODO: make the sink validator run the background and check in parallel as the dataflow progress
    sinkValidator.setExpectRecords(sourceGenerator.getNumberOfGeneratedRecords());
    sinkValidator.run();
    if(sinkValidator.canWaitForTermination()){
      sinkValidator.waitForTermination();
    }
    
    report(shell, sourceGenerator, sinkValidator) ;
    if(dumpRegistry) {
      shell.execute("registry dump");
      shell.execute("dataflow info --dataflow-id " + dataflowId);
    }
  }
  
  protected Thread newPrintDataflowThread(ScribenginShell shell, DataflowDescriptor descriptor) throws Exception {
    return new PrintDataflowInfoThread(shell, descriptor, printDataflowInfo);
  }

  abstract protected void doRun(ScribenginShell shell) throws Exception;

  protected void report(ScribenginShell shell, DataflowSourceGenerator generator, DataflowSinkValidator validator) throws Exception {
    DataflowTestReport report = new DataflowTestReport();
    generator.populate(report);
    validator.populate(report);
    shell.console().println(report.getFormatedReport());
    junitReport(report);
  }

  protected void report(ScribenginShell shell, DataflowWaitingEventListener waitingEventListener) throws IOException {
    TabularFormater dataflowEventInfo = waitingEventListener.getTabularFormaterEventLogInfo();
    dataflowEventInfo.setTitle("Dataflow event log info");
    shell.console().println(dataflowEventInfo.getFormatText());
  }

  protected void setupDebugger(ScribenginShell shell, ScribenginClient scribenginClient, DataflowDescriptor dflDescriptor) throws Exception {
    if(detailedDebugDataflowTask){
      shell.getScribenginClient().getDataflowTaskDebugger(System.out, dflDescriptor, true);
    }
    else if(debugDataflowTask) {
      shell.getScribenginClient().getDataflowTaskDebugger(System.out, dflDescriptor, false);
    }
    
    if(detailedDebugDataflowVM){
      shell.getScribenginClient().getDataflowVMDebugger(System.out, dflDescriptor, true);
    } else if(debugDataflowVM) {
      shell.getScribenginClient().getDataflowVMDebugger(System.out, dflDescriptor, false);
    }
    
    if(detailedDebugDataflowActivity){
      shell.getScribenginClient().getDataflowActivityDebugger(System.out, dflDescriptor, true);
    }
    else if(debugDataflowActivity) {
      shell.getScribenginClient().getDataflowActivityDebugger(System.out, dflDescriptor, false);
    }
  }

  protected void junitReport(DataflowTestReport dataFlowTestReport) throws Exception {
    if (junitReport == null) {
      return;
    }

    DataflowSourceGeneratorReport sourceReport = dataFlowTestReport.getSourceGeneratorReport();
    DataflowSinkValidatorReport sinkReport = dataFlowTestReport.getSinkValidatorReport();
    TestSet testSet = new TestSet();
    int testNum = 0;

    testSet.addTestResult(newTestResult(++testNum,
        "Source Name: " + sourceReport.getSourceName(),
        StringUtils.isNotBlank(sourceReport.getSourceName())));

    testSet.addTestResult(newTestResult(++testNum,
        "Source Streams: " + sourceReport.getNumberOfStreams(),
        sourceReport.getNumberOfStreams() > 0));

    testSet.addTestResult(newTestResult(++testNum,
        "Write Duration (ms): " + sourceReport.getDuration(),
        sourceReport.getDuration() > 0.0));

    testSet.addTestResult(newTestResult(++testNum,
        "Write Count: " + sourceReport.getWriteCount(),
        sourceReport.getWriteCount() > 0));

    testSet.addTestResult(newTestResult(++testNum,
        "Sink Name: " + sinkReport.getSinkName(),
        StringUtils.isNotBlank(sinkReport.getSinkName())));

    testSet.addTestResult(newTestResult(++testNum,
        "Sink Streams: " + sinkReport.getNumberOfStreams(),
        sinkReport.getNumberOfStreams() > 0));

    testSet.addTestResult(newTestResult(++testNum,
        "Read Duration (ms): " + sinkReport.getDuration(),
        sinkReport.getDuration() > 0));

    testSet.addTestResult(newTestResult(++testNum,
        "Read Count: " + sinkReport.getReadCount(),
        sinkReport.getReadCount() > 0));

    testSet.addTestResult(newTestResult(++testNum,
        "Overall Test success : " + (sinkReport.getReadCount() >= sourceReport.getWriteCount()),
        sinkReport.getReadCount() >= sourceReport.getWriteCount()));

    TapProducer tapProducer = TapProducerFactory.makeTapJunitProducer(junitReport);
    tapProducer.dump(testSet, new File(junitReport));
  }

  private TestResult newTestResult(int testNum, String desc, boolean passed) {
    TestResult tr = null;
    if (passed) {
      tr = new TestResult(StatusValues.OK, testNum);
    } else {
      tr = new TestResult(StatusValues.NOT_OK, testNum);
    }
    tr.setDescription(desc);
    return tr;
  }

  
  
  public static class PrintDataflowInfoThread extends Thread {
    ScribenginShell shell;
    DataflowDescriptor descriptor;
    long period;

    PrintDataflowInfoThread(ScribenginShell shell, DataflowDescriptor descriptor, long period) {
      this.shell = shell;
      this.descriptor = descriptor;
      this.period = period;
    }

    public void run() {
      if (period < 1)
        return;
      try {
        while (true) {
          Thread.sleep(period);
          try {
            shell.console().println("#Dataflow Print Thread failurePeriod = " + period + "#");
            shell.execute("dataflow info --dataflow-id " + descriptor.getId());
          } catch (Exception ex) {
            System.err.println(ex.getMessage());
          }
        }
      } catch (InterruptedException ex) {
      }
    }
  }

  static public class TestCopyScribe extends ScribeAbstract {
    private int count = 0;

    @Override
    public void process(Record record, DataflowTaskContext ctx) throws Exception {
      ctx.append(record);
      count++;
      if (count == 100) {
        ctx.commit();
        count = 0;
      }
    }
  }
}