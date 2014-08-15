package com.neverwinterdp.scribengin;

import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.beust.jcommander.JCommander;


/**
 * Unit test for simple App.
 */
public class AppTest
  extends TestCase
{
  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public AppTest( String testName )
  {
    super( testName );
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite()
  {
    return new TestSuite( AppTest.class );
  }

  public void testPlaceHolder() {
  }

  //public void testPythonStreamingAM_container1_2container()
  //{
    //List<String> cmdLst = testPythonStreamingAMHelper(0, 1, 2);
    //assertEquals(cmdLst.get(0), "/opt/kxae/current/bin/import_runner.py -f envelop -i batches_valid -P 1,3,5,7,9,11,13,15,17,19 -H");
  //}

  //public void testPythonStreamingAM_container2_2container()
  //{
    //List<String> cmdLst = testPythonStreamingAMHelper(1, 1, 2);
    //assertEquals(cmdLst.get(0), "/opt/kxae/current/bin/import_runner.py -f envelop -i batches_valid -P 2,4,6,8,10,12,14,16,18,20 -H");
  //}

  //public void testPythonStreamingAM_alloc2_2container()
  //{
    //List<String> cmdLst = testPythonStreamingAMHelper(0, 2, 2);
    //assertEquals(cmdLst.get(0), "/opt/kxae/current/bin/import_runner.py -f envelop -i batches_valid -P 1,3,5,7,9,11,13,15,17,19 -H");
    //assertEquals(cmdLst.get(1), "/opt/kxae/current/bin/import_runner.py -f envelop -i batches_valid -P 2,4,6,8,10,12,14,16,18,20 -H");
  //}

  //public void testPythonStreamingAM_container3_2container()
  //{
    //List<String> cmdLst = testPythonStreamingAMHelper(2, 1, 2);
    //assertEquals(0, cmdLst.size());
  //}

  //private List<String> testPythonStreamingAMHelper(int startingFrom, int allocatedcontainerCnt, int totalContainerCnt) {
    //PythonStreamingAM am = new PythonStreamingAM();
    //String[] args = {
      //"--" + Constants.OPT_CONTAINER_MEM , "128",
      //"--" + Constants.OPT_CONTAINER_COUNT , Integer.toString(totalContainerCnt),
      //"--" + Constants.OPT_COMMAND , "/opt/kxae/current/bin/import_runner.py -f envelop -i batches_valid -P {{n}} -H",
    //};
    //new JCommander(am, args);
    //am.init(args);

    //List<String> commandList = am.buildCommandList(startingFrom, allocatedcontainerCnt, "/opt/kxae/current/bin/import_runner.py -f envelop -i batches_valid -P {{n}} -H");
    //return commandList;
  //}

}
