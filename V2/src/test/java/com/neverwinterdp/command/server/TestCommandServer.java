package com.neverwinterdp.command.server;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.neverwinterdp.jetty.JettyServer;

public class TestCommandServer {
  private static JettyServer httpServer;
  private static int port = 8181;
  
  @BeforeClass
  public static void setup() throws Exception{
    httpServer = new JettyServer(port, CommandServlet.class);
    httpServer.start();
  }
  
  @AfterClass
  public static void teardown() throws Exception{
    //Uncomment this line if you want the server to not exit
    //httpServer.join();
    
    httpServer.stop();
  }
  
  @Test
  public void testCommandServletListVMs() throws InterruptedException, UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(port))
           .field("command", "listvms")
           .asString();
    
    assertEquals("command run: "+"listvms", resp.getBody());
  }
  
  @Test
  public void testCommandServletBadCommand() throws InterruptedException, UnirestException{
    String badCommand = "xxyyzz";
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(port))
           .field("command", badCommand)
           .asString();
    
    assertEquals(CommandServlet.badCommandMessage+badCommand, resp.getBody());
  }
  
  @Test
  public void testCommandServletNoCommand() throws UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(port))
        .asString();
    
    assertEquals(CommandServlet.noCommandMessage, resp.getBody());
  }
}
