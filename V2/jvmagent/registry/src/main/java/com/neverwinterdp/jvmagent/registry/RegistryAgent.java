package com.neverwinterdp.jvmagent.registry;

import java.lang.instrument.Instrumentation;
import java.util.Properties;

import com.neverwinterdp.jvmagent.bootstrap.BootstrapAgentPlugin;

public class RegistryAgent implements BootstrapAgentPlugin {
  private RegistryAgentConfig config ;
  private JVMRegistry jvmRegistry;
  
  public void run(Properties props, Instrumentation inst) {
    config = new RegistryAgentConfig(props);
    RegistryAgentRunner runner = new RegistryAgentRunner();
    runner.start();
  }
  
  class RegistryAgentRunner extends Thread {
    public void run() {
      for(int i = 0; i < 5; i++) {
        Exception error = null;
        try { 
          jvmRegistry = new JVMRegistry(config);
          jvmRegistry.connect(5000);
          JVMInfo jvmInfo = new JVMInfo() ;
          jvmRegistry.create(jvmInfo);
          System.out.println(jvmInfo);
        } catch(Exception ex) {
          error = ex;
          ex.printStackTrace();
        }
        if(error == null) return ;
        System.err.println("Error: " + error.getMessage());
      }
    }
  }
}
