package com.neverwinterdp.scribengin.dataflow.ri;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginUnitTest;
import com.neverwinterdp.scribengin.dataflow.DataflowConfig;
import com.neverwinterdp.scribengin.master.Master;

public class DataflowMasterUnitTest extends ScribenginUnitTest {
  private Master master ;
  
  @Before
  public void setup() throws Exception {
    super.setup();
    master = newMasterContainer().getInstance(Master.class) ;
    master.start();
  }
  
  @After
  public void teardown() throws Exception {
    master.stop();
    super.teardown();
  }
  
  @Test
  public void testDataflowMasterUnitTest() throws Exception {
    DataflowConfig config = new DataflowConfig();
    master.submit(config);
  }
}
