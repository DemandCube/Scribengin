package com.neverwinterdp.jvmagent.bootstrap;

import org.junit.Test;

public class BootstrapAgentUnitTest {
  @Test
  public void testBootstrap() throws Exception {
    BootstrapAgent.premain("src/test/resources/plugin", null);
  }
}