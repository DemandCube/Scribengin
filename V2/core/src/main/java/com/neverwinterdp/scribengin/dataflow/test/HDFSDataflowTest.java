package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;

public class HDFSDataflowTest extends DataflowTest {
  final static public String TEST_NAME = "hdfs";
  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new HDFSDataflowSourceGenerator();

  @ParametersDelegate
  private DataflowSinkValidator sinkValidator = new HDFSDataflowSinkValidator();

  protected void doRun(ScribenginShell shell) throws Exception {
    System.err.println("do we ever get here.1");
    ScribenginClient scribenginClient = shell.getScribenginClient();
    sourceGenerator.init(scribenginClient);
    sourceGenerator.run();
    System.err.println("do we ever get here.");
    sinkValidator.init(scribenginClient);

    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName(dataflowName);
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setTaskMaxExecuteTime(taskMaxExecuteTime);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());

    dflDescriptor.setSourceDescriptor(sourceGenerator.getSourceDescriptor());

    dflDescriptor.addSinkDescriptor("default", sinkValidator.getSinkDescriptor());

    printDebugInfo(shell, scribenginClient, dflDescriptor);

    sinkValidator.setExpectRecords(sourceGenerator.getNumberOfGeneratedRecords());
    sinkValidator.run();

    report(shell, sourceGenerator, sinkValidator);

    if (dumpRegistry) {
      shell.execute("registry dump");
    }
  }
}