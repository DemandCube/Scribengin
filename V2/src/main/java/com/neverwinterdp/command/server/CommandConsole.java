package com.neverwinterdp.command.server;

import java.io.IOException;

import com.neverwinterdp.vm.client.shell.Console;

public class CommandConsole extends Console{
  StringBuilder lastCommand = new StringBuilder();
  
  public void print(String line) throws IOException {
    out.append(line);
    lastCommand.append(line);
  }
  
  public void println(String line) throws IOException {
    out.append(line).append('\n');
    lastCommand.append(line).append('\n');
  }
  
  public String getLastCommandsOutput(){
    String res = new String(lastCommand.toString());
    lastCommand.setLength(0);
    return res;
  }
}
