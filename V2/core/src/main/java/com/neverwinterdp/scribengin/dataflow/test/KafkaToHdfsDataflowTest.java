package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;

public class KafkaToHdfsDataflowTest extends DataflowTest {

  public static final String TEST_NAME = "kafka-to-hdfs";

  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new KafkaDataflowSourceGenerator();

  @ParametersDelegate
  private DataflowSinkValidator sinkValidator = new HDFSDataflowSinkValidator();

  protected void doRun(ScribenginShell shell) throws Exception {
    sourceToSinkDataflowTest(shell, sourceGenerator, sinkValidator);
  }
}