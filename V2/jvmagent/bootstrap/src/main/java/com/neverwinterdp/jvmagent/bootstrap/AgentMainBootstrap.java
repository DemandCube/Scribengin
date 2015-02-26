package com.neverwinterdp.jvmagent.bootstrap;

import java.lang.instrument.Instrumentation;

public class AgentMainBootstrap {
  public static void agentmain(String pluginPath, Instrumentation inst) throws Exception {
    System.out.println("call agentmain.................");
  }
}