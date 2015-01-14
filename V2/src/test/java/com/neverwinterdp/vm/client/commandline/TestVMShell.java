package com.neverwinterdp.vm.client.commandline;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;

public class TestVMShell {
  static VMUnitTest testHelper;
  static Shell shell;
  static VMClient vmClient;
  
  @BeforeClass
  public static void setup() throws Exception {
    testHelper = new VMUnitTest();
    testHelper.setup();

    Thread.sleep(2000);
    shell = testHelper.newShell();
    vmClient = shell.getVMClient();
  }
  
  @AfterClass
  public static void teardown() throws Exception {
    testHelper.teardown();
  }
  
  @Test
  public void testClient() throws Exception{
    shell.execute("vm list");
  }
}
