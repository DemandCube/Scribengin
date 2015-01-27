package com.neverwinterdp.command.server;

import com.neverwinterdp.scribengin.dataflow.builder.HelloKafkaDataflowBuilder.TestCopyDataProcessor;


public class DescriptorBuilderDefaults {
  public static final String _dataflowName          = "defaultDataFlow";
  public static final int    _numWorkers            = 1;
  public static final int    _numExecutorsPerWorker = 1;
  public static final String _dataProcessorClass    = TestCopyDataProcessor.class.getName();
  
  public static final String _kafkaName       = "KAFKA";
  public static final String _kafkaTopic      = "defaultTopic";
  public static final String _kafkaZkConnect  = "127.0.0.1:2181";
  public static final String _kafkaBrokerList = "127.0.0.1:9092";
  
}
