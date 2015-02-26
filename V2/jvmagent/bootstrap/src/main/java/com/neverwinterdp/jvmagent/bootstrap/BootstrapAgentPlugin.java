package com.neverwinterdp.jvmagent.bootstrap;

import java.lang.instrument.Instrumentation;
import java.util.Properties;

public interface BootstrapAgentPlugin {
  public void premain(Properties props, Instrumentation inst) throws Exception ;
  
  public void agentmain(Properties props, Instrumentation inst) throws Exception ;
}