package com.neverwinterdp.vm.client.shell;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.vm.client.VMClient;

public class Shell {
  protected Console console ;
  protected VMClient vmClient ;
  protected Map<String, Command> commands = new HashMap<String, Command>() ;
  
  public Shell(VMClient vmClient) {
    this(vmClient, new Console());
  }
  
  public Shell(VMClient vmClient, Console console){
    this.console = console ;
    this.vmClient = vmClient;
    add("registry", new RegistryCommand());
    add("vm", new VMCommand());
  }
  
  public Console console() { return this.console ; }
  
  public VMClient getVMClient() { return this.vmClient ; }
  
  public void add(String name, Command command) {
    commands.put(name, command);
  }
  
  public void execute(String cmdLine) throws Exception {
    CommandInput cmdInput = new CommandInput(cmdLine, true) ;
    Command command = commands.get(cmdInput.getCommand());
    if(command == null) {
      throw new Exception("Unkown command " + cmdInput.getCommand()) ;
    }
    command.execute(this, cmdInput);
  }
}
