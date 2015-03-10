package com.neverwinterdp.jvmagent.bootstrap;

import java.lang.instrument.Instrumentation;
import java.util.Properties;

public interface BootstrapAgentPlugin {
  public void run(Properties props, Instrumentation inst) throws Exception ;
}