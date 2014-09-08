package com.neverwinterdp.scribengin.writers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginContext;
import com.neverwinterdp.scribengin.MiniDfsClusterBuilder.MiniDfsClusterBuilder;

public class HDFSWriterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/main/resources/log4j.properties") ;
  }
  
  private static MiniDfsClusterBuilder hadoopServer;
  private static String hadoopConnection;
  private static String hdfsPath="test";
  
  @Before
  public void setup() throws Exception {
    hadoopServer = new MiniDfsClusterBuilder();
    hadoopConnection = hadoopServer.build("test");
    System.out.println("Hadoop test server at: "+hadoopConnection);
  }
  
  @After
  public void teardown() {
    hadoopServer.destroy();
  }
  
  @Test
  public void testHDFSWriterDefaultConstructor(){
    ScribenginContext con = new ScribenginContext();
    Properties p = new Properties();
    
    //Where it gets written
    con.setHDFSPath(hadoopConnection+hdfsPath);
    con.setProps(p);
    doTest(con, "NeverwinterDP is snazzy");
  }
  
  @Test
  public void testHDFSWriter(){
    ScribenginContext con = new ScribenginContext();
    Properties p = new Properties();
    //Create this property to trigger other constructor
    p.put("hadoop.configFiles", "/etc/hadoop/conf/hdfs-site.xml,/etc/hadoop/conf/core-site.xml");
    
    //Where it gets written
    con.setHDFSPath(hadoopConnection+hdfsPath);
    con.setProps(p);
    doTest(con, "NeverwinterDP is cool");
  }
  
  
  
  public void doTest(ScribenginContext con, String s){
    byte[] b = s.getBytes();
    Properties p = new Properties();
    
    //Where it gets written
    con.setHDFSPath(hadoopConnection+hdfsPath);
    con.setProps(p);
    
    HDFSWriter w = new HDFSWriter(con);
    
    //Write to HDFS
    try {
      w.write(b);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Write to HDFS failed",false);
    }
    
    //Read in what was just written
    String readLine="";
    try {
      FileSystem fs = FileSystem.get(URI.create(hadoopConnection+hdfsPath), new Configuration());
      Path path = new Path(hadoopConnection+hdfsPath);
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
      readLine=br.readLine();
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Could not read from HDFS", false);
    }
    
    //Assert what's read is what was written
    assertEquals(s,readLine);
  }
}
