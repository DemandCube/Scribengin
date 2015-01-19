package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.client.ScribenginClient;
import com.neverwinterdp.vm.client.shell.Console;
import com.neverwinterdp.vm.client.shell.Shell;

public class ScribenginShell extends Shell {
  private ScribenginClient scribenginClient;
  
  public ScribenginShell(Registry registry) {
    this(registry, new Console());
  }
  
  public ScribenginShell(Registry registry, Console console){
    super(registry, console);
    this.scribenginClient = new ScribenginClient(registry);
    add("scribengin", new ScribenginCommand());
  }

  public ScribenginClient getScribenginClient() { return this.scribenginClient;  }
}
