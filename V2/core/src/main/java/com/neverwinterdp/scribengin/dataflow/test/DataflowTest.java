package com.neverwinterdp.scribengin.dataflow.test;

import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.tap4j.model.TestResult;
import org.tap4j.model.TestSet;
import org.tap4j.producer.TapProducer;
import org.tap4j.producer.TapProducerFactory;
import org.tap4j.util.StatusValues;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSinkValidatorReport;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSourceGeneratorReport;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;

abstract public class DataflowTest {
  @Parameter(names = "--flow-name", description = "The flow name")
  protected String name = "hello";

  @Parameter(names = "--worker", description = "Number of the workers")
  protected int numOfWorkers = 3;

  @Parameter(names = "--executor-per-worker", description = "The number of executor per worker")
  protected int numOfExecutorPerWorker = 3;

  @Parameter(names = "--task-max-execute-time", description = "The max time an executor should work on a task")
  protected long taskMaxExecuteTime = 10000;

  @Parameter(names = "--duration", description = "Max duration for the test")
  protected long duration = 60000;

  @Parameter(names = "--print-dataflow-info", description = "Max duration for the test")
  protected long printDataflowInfo = 5000;

  @Parameter(names = "--junit-report", description = "The junit report output file")
  protected String junitReport;

  public void run(ScribenginShell shell) throws Exception {
    doRun(shell);
  }

  protected Thread newPrintDataflowThread(ScribenginShell shell, DataflowDescriptor descriptor) throws Exception {
    return new PrintDataflowInfoThread(shell, descriptor, printDataflowInfo);
  }

  abstract protected void doRun(ScribenginShell shell) throws Exception;

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
      if(period < 1) return;
      try {
        while (true) {
          Thread.sleep(period);
          try {
            shell.console().println("#Dataflow Print Thread period = " + period + "#"); ;
            shell.execute("dataflow info --running " + descriptor.getName());
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
      count++ ;
      if(count == 100) {
        ctx.commit();
        count = 0;
      }
    }
  }
}
