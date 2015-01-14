package com.neverwinterdp.command.server;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class CommandServlet extends HttpServlet {
  public static String noCommandMessage = "No Command Sent";
  public static String badCommandMessage = "Bad Command: ";
  
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
          response.getWriter().print("command run: "+command);
          break;
        default:
          response.getWriter().print(badCommandMessage+command);
      }
    }
  }
}
