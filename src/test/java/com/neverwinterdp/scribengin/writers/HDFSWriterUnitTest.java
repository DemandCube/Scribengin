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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginContext;
import com.neverwinterdp.scribengin.MiniDfsClusterBuilder.MiniDfsClusterBuilder;

/**
 * Makes sure HDFSWriter can write and append successfully to HDFS
 * @author Richard Duarte
 */
public class HDFSWriterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/main/resources/log4j.properties") ;
  }
  
  private static MiniDfsClusterBuilder hadoopServer;
  private static String hadoopConnection;
  
  
  @BeforeClass
  public static void setup() throws Exception {
    hadoopServer = new MiniDfsClusterBuilder();
    hadoopConnection = hadoopServer.build();
    System.out.println("Hadoop test server at: "+hadoopConnection);
  }
  
  @AfterClass
  public static void teardown() {
    hadoopServer.destroy();
  }
  
  /**
   * Creates new writer, creates file and writes to it, then appends to that file. 
   */
  @Test
  public void testHDFSWriterAppend(){
    String testString = "flubbery";
    byte[] testStringBytes = testString.getBytes();
    
    String hdfsPath = "testAppend";
    
    ScribenginContext context = new ScribenginContext();
    Properties props = new Properties();
    
    //Where it gets written
    context.setHDFSPath(hadoopConnection+hdfsPath);
    context.setProps(props);
    
    HDFSWriter writer = new HDFSWriter(context);
    
    //Write to HDFS
    try {
      writer.write(testStringBytes);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Write to HDFS failed",false);
    }
    //Append to same file
    try {
      writer.write(testStringBytes);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Append to existing HDFS file failed",false);
    }
    
    assertEquals(testString+testString,getFileHDFS(hdfsPath));
  }

  
  /**
   * Creates new writer, writes to HDFS, confirms string is correct
   */
  @Test
  public void testHDFSWriter(){
    String testString = "snazzy";
    byte[] testStringBytes = testString.getBytes();
    
    String hdfsPath = "testWrite";
    
    ScribenginContext context = new ScribenginContext();
    Properties props = new Properties();
    //Create this property to trigger other constructor
    props.put("hadoop.configFiles", "/etc/hadoop/conf/hdfs-site.xml,/etc/hadoop/conf/core-site.xml");
    
    //Where it gets written
    context.setHDFSPath(hadoopConnection+hdfsPath);
    context.setProps(props);
    
    HDFSWriter writer = new HDFSWriter(context);
    
    //Write to HDFS
    try {
      writer.write(testStringBytes);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Write to HDFS failed",false);
    }
    
    assertEquals(testString, getFileHDFS(hdfsPath));
  }
  
  
  /**
   * Read in file, return whole file as a string
   * @param hdfsPath Path of HDFS file to read
   * @return whole file as a string
   */
  private String getFileHDFS(String hdfsPath) {
    String readLine="";
    String tempLine="";
    try {
      FileSystem fs = FileSystem.get(URI.create(hadoopConnection+hdfsPath), new Configuration());
      Path path = new Path(hadoopConnection+hdfsPath);
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
      while((tempLine = br.readLine() ) != null){
        readLine+=tempLine;
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Could not read from HDFS", false);
    }
    return readLine;
  }
  
  
}
