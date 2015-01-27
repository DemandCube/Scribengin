package com.neverwinterdp.command.server;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.vm.client.shell.Shell;

@SuppressWarnings("serial")
public class CommandServlet extends HttpServlet {
  public static String noCommandMessage = "No Command Sent";
  public static String badCommandMessage = "Bad Command: ";
  private Shell vmShell; 
  private CommandConsole shellConsole;


  @Override
  public void init() throws ServletException {
    RegistryConfig regConf = new RegistryConfig();
    ServletConfig conf = this.getServletConfig();
    
    //Get config from web.xml
    regConf.setConnect(conf.getInitParameter("host"));
    regConf.setDbDomain("/NeverwinterDP");
    
    shellConsole = new CommandConsole();
    
    try {
      vmShell = new ScribenginShell(new RegistryImpl(regConf).connect(), shellConsole);
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    
  }
  
  @Override
  protected void doGet(HttpServletRequest request,
                       HttpServletResponse response ) throws ServletException,IOException{
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().print("Hello from commandServlet");
  }
  
  
  @Override 
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException{
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    
    String command = request.getParameter("command");
    
    /*
    @SuppressWarnings("unchecked")
    Enumeration<String> x = request.getParameterNames();
    while(x.hasMoreElements()){
      System.err.println(x.nextElement());
    }
    
    @SuppressWarnings("unchecked")
    x = request.getHeaderNames();
    while(x.hasMoreElements()){
      System.err.println(x.nextElement());
    }
    
    @SuppressWarnings("unchecked")
    x = request.getAttributeNames();
    while(x.hasMoreElements()){
      System.err.println(x.nextElement());
    }
    System.err.println("");
    */
    
    if(command == null){
      response.getWriter().print(noCommandMessage);
    }
    else{
      switch(command){
        case "vm list":
          response.getWriter().print(executeShell("vm list"));
          break;
        case "registry dump":
          String path = request.getParameter("path");
          if(path == null){
            response.getWriter().print(executeShell("registry dump"));
          }
          else{
            response.getWriter().print(executeShell("registry dump --path "+path));
          }
          break;
        case "scribengin master":
          response.getWriter().print(executeShell("scribengin master"));
          break;
        case "dataflow":
          DescriptorBuilder.parseDataflowInput(request);
          response.getWriter().print("dataflow");
          break;
        default:
          response.getWriter().print(badCommandMessage+command);
      }
    }
  }
  
  protected String executeShell(String command){
    try {
      vmShell.execute(command);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return shellConsole.getLastCommandsOutput();
  }
  
  
}
