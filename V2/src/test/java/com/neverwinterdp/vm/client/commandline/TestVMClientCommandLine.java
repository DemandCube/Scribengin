package com.neverwinterdp.vm.client.commandline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.io.output.NullOutputStream;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.VMScribenginSingleJVMUnitTest;

public class TestVMClientCommandLine extends VMScribenginSingleJVMUnitTest{
  
  private String VMMasterName = "TestMaster1";
  
  @Override @Before
  public void setup() throws Exception {
    PrintStream stdout = System.out;
    try{
      //Suppressing noisy output from the setup methods
      System.setOut(new PrintStream(new NullOutputStream()));
      super.setup();
      Thread.sleep(3000);
      createVMMaster(VMMasterName);
      Thread.sleep(3000);
    }finally{
      //Setting output back to stdout
      System.setOut(stdout);
    }
  }
  
  //Overriding to not have to run this test from base class here
  //@Override @Ignore
  //public void testMaster() {}
  
  
  //@Override
  //public void testVMClientCommandLine() throws RegistryException, IOException{
  //Just overriding the base class test name to only run the one test
  @Override
  public void testMaster() throws RegistryException, IOException{
    PrintStream stdout = System.out;
    String args[] = {"--registry", "localhost:2181",
                      "--getRegistry"
                     };
    try{
      //Capture stdout to a output stream
      ByteArrayOutputStream capturedStdout = new ByteArrayOutputStream();
      System.setOut(new PrintStream(capturedStdout));
      VMClientCommandLine.main(args);
      System.out.flush();
      
      assertFalse(capturedStdout.toString().isEmpty());
    }finally{
      System.setOut(stdout);
    }
    
  }
  

  @Test
  public void testGetDump(){
    RegistryConfig regConf = new RegistryConfig();
    regConf.setConnect("127.0.0.1:2181");
    regConf.setDbDomain("/NeverwinterDP");
    VMClientCommandLine client = new VMClientCommandLine(new RegistryImpl(regConf));
    assertNotNull(client.getDump());
    assertNotNull(client.getDump("/",""));
    assertNotNull(client.getDump("/", "", " "));
  }
  
  
  @Test
  public void testVMDescriptorConversion(){
    RegistryConfig regConf = new RegistryConfig();
    regConf.setConnect("127.0.0.1:2181");
    regConf.setDbDomain("/NeverwinterDP");
    VMClientCommandLine client = new VMClientCommandLine(new RegistryImpl(regConf));
    assertEquals(
        "/vm/allocated/"+VMMasterName,
        client.parseStringToVMDescriptor(client.getRegistryValue("/vm/allocated/"+VMMasterName)).getStoredPath()
      );
    
    assertFalse(
        client.getJsonString(
            client.parseStringToVMDescriptor(
                client.getRegistryValue("/vm/allocated/"+VMMasterName))).isEmpty());
  }
  
  @Test
  public void testGetRegistryData(){
    RegistryConfig regConf = new RegistryConfig();
    regConf.setConnect("127.0.0.1:2181");
    regConf.setDbDomain("/NeverwinterDP");
    VMClientCommandLine client = new VMClientCommandLine(new RegistryImpl(regConf));
    assertEquals(
        "\"RUNNING\"",
        client.getRegistryValue("/vm/allocated/"+VMMasterName+"/status")
      );
    
    assertNull(client.getRegistryValue("/vm/allocated/XXX"));
  }
  
}
