package com.neverwinterdp.vm.client.shell;

import java.util.Map;
import java.util.TreeMap;

public abstract class Command {
  private Map<String, Class<? extends SubCommand>> subcommands = new TreeMap<>() ;

  public void add(String name, Class<? extends SubCommand> type) {
    subcommands.put(name, type) ;
  }
  
  public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    Class<? extends SubCommand> type = subcommands.get(cmdInput.getSubCommand()) ;
    System.out.println("type "+ type);
    SubCommand subcommand = type.newInstance();
    if(subcommand == null) {
      throw new Exception("Unkown sub command for: " + cmdInput.getCommandLine()) ;
    }
    cmdInput.mapRemainArgs(subcommand);
    subcommand.execute(shell, cmdInput);
  }

  public Map<String, Class<? extends SubCommand>> getSubcommands() {
    return subcommands;
  }
  
  public abstract String getDescription();
  
}
