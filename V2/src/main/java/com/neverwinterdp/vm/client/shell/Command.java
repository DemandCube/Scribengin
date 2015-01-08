package com.neverwinterdp.vm.client.shell;

import java.util.HashMap;
import java.util.Map;

public class Command {
  private Map<String, SubCommand> subcommands = new HashMap<String, SubCommand>() ;

  public void add(String name, SubCommand subcommand) {
    subcommands.put(name, subcommand) ;
  }
  
  public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    SubCommand subcommand = subcommands.get(cmdInput.getSubCommand()) ;
    if(subcommand == null) {
      throw new Exception("Unkown sub command for: " + cmdInput.getCommandLine()) ;
    }
    cmdInput.mapRemainArgs(subcommand);
    subcommand.execute(shell, cmdInput);
  }
}
