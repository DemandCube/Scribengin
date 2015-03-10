package com.neverwinterdp.jvmagent.bootstrap;

import org.junit.Test;

public class BootstrapAgentUnitTest {
  @Test
  public void testBootstrap() throws Exception {
    PremainBootstrap.premain("src/test/resources/plugin", null);
  }
}