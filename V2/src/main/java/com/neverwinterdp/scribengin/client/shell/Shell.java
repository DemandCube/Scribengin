package com.neverwinterdp.scribengin.client.shell;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.client.RegistryClient;
import com.neverwinterdp.scribengin.registry.Registry;

public class Shell {
  private Console console ;
  private RegistryClient client ;
  private Map<String, Command> commands = new HashMap<String, Command>() ;
  
  public Shell(Registry registry) {
    this.console = new Console() ;
    client = new RegistryClient(registry);
    commands.put("master", new MasterCommand());
  }
  
  public Console console() { return this.console ; }
  
  public RegistryClient getRegistryClient() { return this.client ; }
  
  public void execute(String cmdLine) throws Exception {
    CommandInput cmdInput = new CommandInput(cmdLine, true) ;
    Command command = commands.get(cmdInput.getCommand());
    if(command == null) {
      throw new Exception("Unkown command " + cmdInput.getCommand()) ;
    }
    command.execute(this, cmdInput);
  }
}
