package com.neverwinterdp.command.server;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

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
  public void testCommandServletCreateDataFlow() throws UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(CommandServletUnitTest.port))
        .field("command", "dataflow")
        .asString();
    
    assertEquals("dataflow", resp.getBody());
    
    
    resp = Unirest.post("http://localhost:"+Integer.toString(CommandServletUnitTest.port))
        .field("command", "registry dump")
        .asString();
    
    System.err.println(resp.getBody());
  }

}
