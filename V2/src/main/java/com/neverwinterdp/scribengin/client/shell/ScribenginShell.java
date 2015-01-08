package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.ScribenginMaster;
import com.neverwinterdp.scribengin.client.ScribenginClient;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.shell.Shell;

public class ScribenginShell extends Shell {
  private ScribenginClient scribenginClient;
  
  public ScribenginShell(Registry registry) {
    super(registry);
    this.scribenginClient = new ScribenginClient(registry);
    add("scribengin", new ScribenginCommand());
  }

  public ScribenginClient getScribenginClient() { return this.scribenginClient;  }
}
