package com.neverwinterdp.command.server;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;

public class CommandServletDataFlowUnitTest {
  
  @BeforeClass
  public static void setup() throws Exception{
    CommandServletUnitTest.setup();
  }
  
  @AfterClass
  public static void teardown() throws Exception{
    CommandServletUnitTest.teardown();
  }
  
  @Test
  public void testCommandServletCreateDataFlowDefault() throws UnirestException, RegistryException, JsonParseException, JsonMappingException, IOException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(CommandServletUnitTest.port))
        .field("command", "dataflow")
        .asString();
    
    assertEquals("DATAFLOW "+DescriptorBuilderDefaults._dataflowName +" SUBMITTED SUCCESSFULLY", resp.getBody());
    
    Registry r = CommandServerTestBase.getNewRegistry();
    r.connect();
    Node x = r.get("/scribengin/dataflows/running/defaultDataFlow");

    ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally
    DataflowDescriptor dfDesc = mapper.readValue(new String(x.getData()), DataflowDescriptor.class);
    
    assertEquals(DescriptorBuilderDefaults._dataflowName, dfDesc.getName());
    assertEquals("KAFKA", dfDesc.getSourceDescriptor().getType());
    
    Map<String, StorageDescriptor> sinks = dfDesc.getSinkDescriptors();
    
    for (Entry<String, StorageDescriptor> entry : sinks.entrySet()) {
      assertEquals("KAFKA", entry.getValue().getType());
    }
    
    assertEquals(DescriptorBuilderDefaults._dataflowName, dfDesc.getName());
    assertEquals(DescriptorBuilderDefaults._dataflowName, dfDesc.getName());
    
  }
  
  @Test
  public void testCommandServletCreateDataFlow() throws UnirestException, RegistryException, JsonParseException, JsonMappingException, IOException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(CommandServletUnitTest.port))
        .field("command", "dataflow")
        .field("dataflow-Name"                  , "dataflowName")
        .field("dataflow-Dataprocessor"         , "dataflowDataprocessor")
        .field("dataflow-NumWorkers"            , "10")
        .field("dataflow-NumExecutorsPerWorkers", "20")
        .field("source-Type"      , "KAFKA")
        .field("source-Name"      , "sinkName")
        .field("source-Topic"     , "sourceTopic")
        .field("source-ZkConnect" , "sourceZkConnect")
        .field("source-BrokerList", "sourceBrokerList")
        .field("sink-Type"      , "KAFKA")
        .field("sink-Name"      , "sinkName")
        .field("sink-Topic"     , "sinkTopic")
        .field("sink-ZkConnect" , "sinkZkConnect")
        .field("sink-BrokerList", "sinkBrokerList")
        .field("invalidsink-Type"      , "KAFKA")
        .field("invalidsink-Name"      , "invalidsinkName")
        .field("invalidsink-Topic"     , "invalidsinkTopic")
        .field("invalidsink-ZkConnect" , "invalidsinkZkConnect")
        .field("invalidsink-BrokerList", "invalidsinkBrokerList")
        .asString();
    
    
    assertEquals("DATAFLOW dataflowName SUBMITTED SUCCESSFULLY", resp.getBody());
    
    Registry r = CommandServerTestBase.getNewRegistry();
    r.connect();
    Node flowNode = r.get("/scribengin/dataflows/running/dataflowName");
    //System.err.println(new String(flowNode.getData()));
    ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally
    DataflowDescriptor dfDesc = mapper.readValue(new String(flowNode.getData()), DataflowDescriptor.class);
    
    assertEquals(10, dfDesc.getNumberOfWorkers());
    assertEquals(20, dfDesc.getNumberOfExecutorsPerWorker());
    assertEquals("dataflowName", dfDesc.getName());
    assertEquals("dataflowDataprocessor", dfDesc.getScribe());
    

    
    assertEquals("KAFKA", dfDesc.getSourceDescriptor().getType());
    assertEquals("sinkName", dfDesc.getSourceDescriptor().attribute("name"));
    assertEquals("sourceTopic", dfDesc.getSourceDescriptor().attribute("topic"));
    assertEquals("sourceZkConnect", dfDesc.getSourceDescriptor().attribute("zk.connect"));
    assertEquals("sourceBrokerList", dfDesc.getSourceDescriptor().attribute("broker.list"));
    
    Map<String, StorageDescriptor> sinks = dfDesc.getSinkDescriptors();
    StorageDescriptor sink = sinks.get("default");
    StorageDescriptor invalidSink = sinks.get("invalid");
    
    assertEquals("KAFKA", sink.getType());
    assertEquals("sinkName", sink.attribute("name"));
    assertEquals("sinkTopic", sink.attribute("topic"));
    assertEquals("sinkZkConnect", sink.attribute("zk.connect"));
    assertEquals("sinkBrokerList", sink.attribute("broker.list"));
    
    
    assertEquals("KAFKA", invalidSink.getType());
    assertEquals("invalidsinkName", invalidSink.attribute("name"));
    assertEquals("invalidsinkTopic", invalidSink.attribute("topic"));
    assertEquals("invalidsinkZkConnect", invalidSink.attribute("zk.connect"));
    assertEquals("invalidsinkBrokerList", invalidSink.attribute("broker.list"));
    
    
    /*
    resp = Unirest.post("http://localhost:"+Integer.toString(CommandServletUnitTest.port))
        .field("command", "registry dump")
        .asString();
    System.out.println(resp.getBody());
    */
  }
  
  

}
