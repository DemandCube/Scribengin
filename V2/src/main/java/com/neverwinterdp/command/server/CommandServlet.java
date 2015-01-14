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
import com.neverwinterdp.vm.client.shell.Shell;

@SuppressWarnings("serial")
public class CommandServlet extends HttpServlet {
  public static String noCommandMessage = "No Command Sent";
  public static String badCommandMessage = "Bad Command: ";
  private Shell shell; 
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
      shell = new Shell(new RegistryImpl(regConf).connect(), shellConsole) ;
      
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
        case "listvms":
          try {
            shell.execute("vm list");
            response.getWriter().print(shellConsole.getLastCommandsOutput());
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          break;
        default:
          response.getWriter().print(badCommandMessage+command);
      }
    }
  }
}
