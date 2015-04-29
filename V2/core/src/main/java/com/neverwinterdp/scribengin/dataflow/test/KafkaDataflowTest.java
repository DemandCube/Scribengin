package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;

public class KafkaDataflowTest extends DataflowTest {
  
  final static public String TEST_NAME = "kafka-to-kakfa" ;
  
  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new KafkaDataflowSourceGenerator();
  
  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator   = new KafkaDataflowSinkValidator();
  
  protected void doRun(ScribenginShell shell) throws Exception {
    sourceToSinkDataflowTest(shell, sourceGenerator, sinkValidator);
  }
}