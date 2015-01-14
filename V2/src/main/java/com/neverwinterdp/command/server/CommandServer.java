package com.neverwinterdp.command.server;

import com.neverwinterdp.jetty.JettyServer;
import com.neverwinterdp.jetty.servlets.HelloServlet;

public class CommandServer {
  private int port;
  
  public CommandServer(int port){
    this.port = port;
  }
  
  public CommandServer(){
    this(8181);
  }
  
  public void startServer() throws Exception{
    JettyServer httpServer = new JettyServer(port, HelloServlet.class);
    httpServer.start();
  }
}
