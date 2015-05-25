package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;

public class HDFSToKafkaDataflowTest extends DataflowTest {

  public static final String TEST_NAME = "hdfs-to-kafka";

  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new HDFSDataflowSourceGenerator();
  
  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator   = new KafkaDataflowSinkValidator();
  
  protected void doRun(ScribenginShell shell) throws Exception {
    sourceToSinkDataflowTest(shell, sourceGenerator, sinkValidator);
  }
}