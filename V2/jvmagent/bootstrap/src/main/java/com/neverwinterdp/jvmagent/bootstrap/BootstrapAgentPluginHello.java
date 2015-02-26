package com.neverwinterdp.jvmagent.bootstrap;

import java.lang.instrument.Instrumentation;
import java.util.Properties;

public class BootstrapAgentPluginHello implements BootstrapAgentPlugin {

  @Override
  public void premain(Properties props, Instrumentation inst) throws Exception {
    System.out.println("BootstrapAgentPluginHello: hello premain");
    System.out.println("  Classloader: " + Thread.currentThread().getContextClassLoader().hashCode());
  }

  @Override
  public void agentmain(Properties props, Instrumentation inst) throws Exception {
    System.out.println("BootstrapAgentPluginHello: hello premain");
  }
}