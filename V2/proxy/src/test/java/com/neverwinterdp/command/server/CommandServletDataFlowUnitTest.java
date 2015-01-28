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
import com.neverwinterdp.scribengin.sink.SinkDescriptor;

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
    
    assertEquals("DATAFLOW SUBMITTED SUCCESSFULLY", resp.getBody());
    
    Registry r = CommandServerTestHelper.getNewRegistry();
    r.connect();
    Node x = r.get("/scribengin/dataflows/running/defaultDataFlow");

    ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally
    DataflowDescriptor dfDesc = mapper.readValue(new String(x.getData()), DataflowDescriptor.class);
    
    assertEquals(DescriptorBuilderDefaults._dataflowName, dfDesc.getName());
    assertEquals("KAFKA", dfDesc.getSourceDescriptor().getType());
    
    Map<String, SinkDescriptor> sinks = dfDesc.getSinkDescriptors();
    
    for (Entry<String, SinkDescriptor> entry : sinks.entrySet()) {
      assertEquals("KAFKA", entry.getValue().getType());
    }
    
    assertEquals(DescriptorBuilderDefaults._dataflowName, dfDesc.getName());
    assertEquals(DescriptorBuilderDefaults._dataflowName, dfDesc.getName());
    
  }
  
  @Test
  public void testCommandServletCreateDataFlow() throws UnirestException, RegistryException, JsonParseException, JsonMappingException, IOException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(CommandServletUnitTest.port))
        .field("command", "dataflow")
        .field("source-Type", "KAFKA")
        .field("source-Name", "sourceName")
        .field("source-Topic", "sourceTopic")
        .field("source-ZkConnect", "sourceZkConnect")
        .field("source-BrokerList", "sourceBrokerList")
        .asString();
    
    //String type = request.getParameter("source-Type");
    //String name = request.getParameter("source-Name");
    //String topic = request.getParameter("source-Topic");
    //String zkConnect = request.getParameter("source-ZkConnect");
    //String brokerList = request.getParameter("source-BrokerList");
    
    assertEquals("DATAFLOW SUBMITTED SUCCESSFULLY", resp.getBody());
    
    Registry r = CommandServerTestHelper.getNewRegistry();
    r.connect();
    Node x = r.get("/scribengin/dataflows/running/defaultDataFlow");

    ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally
    DataflowDescriptor dfDesc = mapper.readValue(new String(x.getData()), DataflowDescriptor.class);
    
    
    assertEquals(DescriptorBuilderDefaults._dataflowName, dfDesc.getName());
    assertEquals("KAFKA", dfDesc.getSourceDescriptor().getType());
    assertEquals("sourceName", dfDesc.getSourceDescriptor().attribute("name"));
    assertEquals("sourceTopic", dfDesc.getSourceDescriptor().attribute("topic"));
    assertEquals("sourceZkConnect", dfDesc.getSourceDescriptor().attribute("zk.connect"));
    assertEquals("sourceBrokerList", dfDesc.getSourceDescriptor().attribute("broker.list"));
    
    Map<String, SinkDescriptor> sinks = dfDesc.getSinkDescriptors();
    
    for (Entry<String, SinkDescriptor> entry : sinks.entrySet()) {
      assertEquals("KAFKA", entry.getValue().getType());
    }
    
    assertEquals(DescriptorBuilderDefaults._dataflowName, dfDesc.getName());
    assertEquals(DescriptorBuilderDefaults._dataflowName, dfDesc.getName());
    
  }

}
