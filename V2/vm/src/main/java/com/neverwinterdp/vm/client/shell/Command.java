package com.neverwinterdp.vm.client.shell;

import java.util.HashMap;
import java.util.Map;

public class Command {
  private Map<String, Class<? extends SubCommand>> subcommands = new HashMap<>() ;

  public void add(String name, Class<? extends SubCommand> type) {
    subcommands.put(name, type) ;
  }
  
  public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    Class<? extends SubCommand> type = subcommands.get(cmdInput.getSubCommand()) ;
    SubCommand subcommand = type.newInstance();
    if(subcommand == null) {
      throw new Exception("Unkown sub command for: " + cmdInput.getCommandLine()) ;
    }
    cmdInput.mapRemainArgs(subcommand);
    subcommand.execute(shell, cmdInput);
  }
}
