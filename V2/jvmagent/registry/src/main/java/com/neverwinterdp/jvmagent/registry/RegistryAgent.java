package com.neverwinterdp.jvmagent.registry;

import java.lang.instrument.Instrumentation;
import java.util.Properties;

import com.neverwinterdp.jvmagent.bootstrap.BootstrapAgentPlugin;

public class RegistryAgent implements BootstrapAgentPlugin {
  private JVMRegistry jvmRegistry;
  
  public void premain(Properties props, Instrumentation inst) throws Exception {
    RegistryAgentConfig config = new RegistryAgentConfig(props);
    jvmRegistry = new JVMRegistry(config);
    JVMInfo jvmInfo = new JVMInfo() ;
    jvmRegistry.create(jvmInfo);
    System.out.println(jvmInfo);
  }
  
  public void agentmain(Properties props, Instrumentation inst) throws Exception {
    System.out.println("call agentmain ");
  }
}
