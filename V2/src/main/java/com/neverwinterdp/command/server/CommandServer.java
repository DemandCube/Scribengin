package com.neverwinterdp.command.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.jetty.JettyServer;

public class CommandServer {
  public static class CommandLine{
    @Parameter(names = { "-p", "--port" }, description = "Port to run on")
    public int port = 8181;
  }
  
  
  private int port;
  private JettyServer server;
  
  public CommandServer(int port){
    this.port = port;
  }
  
  public CommandServer(){
    this(8181);
  }
  
  public void startServer() throws Exception{
    server = new JettyServer(port, CommandServlet.class);
    server.start();
  }
  
  public void join() throws Exception{
    server.join();
  }
  
  public static void main(String args[]) throws Exception{
    CommandLine  c = new CommandLine();
    
    new JCommander(c, args);
    CommandServer cs = new CommandServer(c.port);
    cs.startServer();
    cs.join();
  }
}
