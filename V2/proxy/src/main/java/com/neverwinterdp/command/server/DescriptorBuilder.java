package com.neverwinterdp.command.server;

import java.util.Map;

import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.SourceDescriptor;

public class DescriptorBuilder {

  protected static DataflowDescriptor parseDataflowInput(Map<String,String> request){
    DataflowDescriptor dflDescriptor = parseDataFlowDescriptor(request);
    
    String sourceType      = request.get("source-Type"); 
    String sinkType        = request.get("sink-Type");
    String invalidSinkType = request.get("invalidsink-Type");
    
    if(sourceType == null){
      sourceType = "KAFKA";
    }
    if(sinkType == null){
      sinkType = "KAFKA";
    }
    if(invalidSinkType == null){
      invalidSinkType = "KAFKA";
    }
    
    switch(sourceType.toUpperCase()){
      case "KAFKA":
        dflDescriptor.setSourceDescriptor(createKafkaSourceDescriptor(request));
        break;
      default:
        break;
    }
    
    switch(sinkType.toUpperCase()){
      case "KAFKA":
        dflDescriptor.addSinkDescriptor("default", createKafkaSinkDescriptor(request, "sink"));
        break;
      default:
        break;
    }
    
    switch(invalidSinkType.toUpperCase()){
      case "KAFKA":
        dflDescriptor.addSinkDescriptor("invalid", createKafkaSinkDescriptor(request, "invalidsink"));
        break;
      default:
        break;
    }
    
    return dflDescriptor;
  }
  
  /**
   * Parse out parameters from httpRequest for dataflowDescriptor
   * If no value is set, pull in defaults
   * @param request
   * @return
   */
  public static DataflowDescriptor parseDataFlowDescriptor(Map<String,String>  request){
    String dataflowName = request.get("dataflow-Name");
    if(dataflowName == null){
      dataflowName = DescriptorBuilderDefaults._dataflowName;
    }
    String dataProcessorClass = request.get("dataflow-Dataprocessor");
    if(dataProcessorClass == null){
      dataProcessorClass = DescriptorBuilderDefaults._dataProcessorClass;
    }
    
    int numWorkers;
    try{
      numWorkers = Integer.parseInt(request.get("dataflow-NumWorkers"));
    }
    catch(Exception e){
      numWorkers = DescriptorBuilderDefaults._numWorkers;
    }
    
    int numExecutorsPerWorker;
    try{
      numExecutorsPerWorker = Integer.parseInt(request.get("dataflow-NumExecutorsPerWorkers"));
    }
    catch(Exception e){
      numExecutorsPerWorker = DescriptorBuilderDefaults._numExecutorsPerWorker;
    }
    
    
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName(dataflowName);
    dflDescriptor.setNumberOfWorkers(numWorkers);
    dflDescriptor.setNumberOfExecutorsPerWorker(numExecutorsPerWorker);
    dflDescriptor.setScribe(dataProcessorClass);
    return dflDescriptor;
  }
  
  /**
   * Parse out details from post request containing source-*
   * and return a Kafka Source Descriptor
   * @param request
   * @return
   */
  public static SourceDescriptor createKafkaSourceDescriptor(Map<String,String>  request){
    String type = request.get("source-Type");
    String name = request.get("source-Name");
    String topic = request.get("source-Topic");
    String zkConnect = request.get("source-ZkConnect");
    String brokerList = request.get("source-BrokerList");
    
    if(type == null){
      type = "KAFKA";
    }
    if(name == null){
      name = DescriptorBuilderDefaults._kafkaName;
    }
    if(topic == null){
      topic = DescriptorBuilderDefaults._kafkaTopic;
    }
    if(zkConnect == null){
      zkConnect = DescriptorBuilderDefaults._kafkaZkConnect;
    }
    if(brokerList == null){
      brokerList = DescriptorBuilderDefaults._kafkaBrokerList;
    }
    
    SourceDescriptor sourceDescriptor = new SourceDescriptor(type) ;
    sourceDescriptor.attribute("name", name);
    sourceDescriptor.attribute("topic", topic);
    sourceDescriptor.attribute("zk.connect", zkConnect);
    sourceDescriptor.attribute("broker.list", brokerList);
    
    return sourceDescriptor;
  }
  
  /**
   * Parse out all the details from the post request matching prefix-*
   * and create a kafka SinkDescriptor
   * @param request
   * @param prefix
   * @return
   */
  public static SinkDescriptor createKafkaSinkDescriptor(Map<String,String>  request, String prefix){
    String type = request.get(prefix+"-Type");
    String name = request.get(prefix+"-Name");
    String topic = request.get(prefix+"-Topic");
    String zkConnect = request.get(prefix+"-ZkConnect");
    String brokerList = request.get(prefix+"-BrokerList");
    
    if(type == null){
      type = "KAFKA";
    }
    if(name == null){
      name = DescriptorBuilderDefaults._kafkaName;
    }
    if(topic == null){
      topic = DescriptorBuilderDefaults._kafkaTopic;
    }
    if(zkConnect == null){
      zkConnect = DescriptorBuilderDefaults._kafkaZkConnect;
    }
    if(brokerList == null){
      brokerList = DescriptorBuilderDefaults._kafkaBrokerList;
    }
    
    SinkDescriptor sink = new SinkDescriptor(type) ;
    sink.attribute("name", name);
    sink.attribute("topic", topic);
    sink.attribute("zk.connect", zkConnect);
    sink.attribute("broker.list", brokerList);
    return sink;
  }
}