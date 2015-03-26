package com.neverwinterdp.vm.client.shell;

import java.io.IOException;

import com.beust.jcommander.JCommander;

//TODO:
//1. move all the HelpCommand format to this class. This class should be moved to other place and reuse by
//   the tool like the check tool
//2. The usage class should allow the user to control the indent level 
//3. For the HelpCommand, you should loop through the subcommand list , use this command usage class to print out.
public class JCommanderUsage<T> {
  private String     indent = "";
  private String     command ;
  private T          commandObj;
  private JCommander jcommander;

  public JCommanderUsage(String command, T commandObj, String[] args) {
    this.command = command;
    this.commandObj = commandObj;
    this.jcommander = new JCommander(commandObj, args);
  }
  
  public void setIndent(String indent) {
    this.indent = indent ;
  }
  
  public String getFormatedUsage() throws IOException {
    StringBuilder b = new StringBuilder() ;
    printUsage(b);
    return b.toString() ;
  }
  
  public void printUsage() throws IOException {
    
  }
  
  public void printUsage(Appendable out) throws IOException {
    
  }
}
