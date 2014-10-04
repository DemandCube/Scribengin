package com.neverwinterdp.scribengin.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;

public class ScribeMasterClusterDistributedDatePartitionerTest {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  private static ScribeConsumerClusterTestHelper helper = new ScribeConsumerClusterTestHelper();
  static int numOfMessages = 200 ;
  private static final Logger LOG = Logger.getLogger(ScribeConsumerClusterTest.class.getName());
  private static Server scribeMaster;
  
  @BeforeClass
  static public void setup() throws Exception {
    helper.setup();
  }

  @AfterClass
  static public void teardown() throws Exception {
    try{
      scribeMaster.destroy();
    } catch(Exception e){}
    helper.teardown();
  }
  
  
  @Test
  public void TestScribeMasterClusterDistributed() throws InterruptedException{
    
    //Bring up scribeMaster
    scribeMaster = Server.create("-Pserver.name=scribemaster", "-Pserver.roles=scribemaster");
    Shell shell = new Shell() ;
    shell.getShellContext().connect();
    shell.execute("module list --type available");
    
    String installScript ="module install " + 
        " -Pmodule.data.drop=true" +
        " -Pscribemaster:topics="+ helper.getTopic() +
        " -Pscribemaster:brokerList=127.0.0.1:9092" +
        " -Pscribemaster:hdfsPath="+helper.getHadoopConnection()+
        " -Pscribemaster:cleanStart=True"+
        " -Pscribemaster:mode=distributed"+
        " -Pscribemaster:date_partitioner=ss"+
        " --member-role scribemaster --autostart --module ScribeMaster \n";
    shell.executeScript(installScript);
    Thread.sleep(2000);
    
    LOG.info("Creating kafka data");
    //Create kafka data
    helper.createKafkaData(0);
    Thread.sleep(2000);
    helper.createKafkaData(100);
    
    //Wait for consumption
    Thread.sleep(10000);
    //Ensure messages 0-100 were consumed
    LOG.info("Asserting data is correct");
    assertHDFSmatchesKafka(0,helper.getHadoopConnection());
  }
  
  
  /**
   * Read in file, return whole file as a string
   * @param hdfsPath Path of HDFS file to read
   * @return whole file as a string
   */
  public void assertHDFSmatchesKafka(int startNum, String hdfsPath) {
    //int count = 0;
    String tempLine="";
    String readLine="";
    int numFiles = 0;
    try {
      FileSystem fs = FileSystem.get(URI.create(hdfsPath), new Configuration());
      Path directory = new Path("/committed/");
      RemoteIterator<LocatedFileStatus> directoryIterator = fs.listFiles(directory,true);
      while(directoryIterator.hasNext()){
        Path p = directoryIterator.next().getPath();
        numFiles++;
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
         while((tempLine = br.readLine() ) != null){
           readLine += tempLine;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Could not read from HDFS", false);
    }
    
    assertEquals("Not enough files were created! Date partitioner messed up", 2, numFiles);
    
    //Build string that will match
    String assertionString = "";
    for(int i=0; i< startNum+numOfMessages; i++){
      assertionString += "Neverwinter"+Integer.toString(i);
    }
    assertEquals("Data passed into Kafka did not match what was read from HDFS",assertionString,readLine);
  }
}
