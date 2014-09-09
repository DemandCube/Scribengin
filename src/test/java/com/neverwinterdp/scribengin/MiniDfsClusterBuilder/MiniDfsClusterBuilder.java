package com.neverwinterdp.scribengin.MiniDfsClusterBuilder;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Builds a Mini HDFS Cluster for testing
 * Default constructor creates 3 datanodes
 * 
 * 
 * Example code:
 * <pre>
 * {@code
 *  MiniDfsClusterBuilder hadoopServer = new MiniDfsClusterBuilder();
 *  hadoopConnection = hadoopServer.build();
 *  System.out.println("Hadoop test server at: "+hadoopConnection);
 *  ....
 *  hadoopServer.destroy();
 *  </pre>
 *  
 *  @author Richard Duarte
 */
public class MiniDfsClusterBuilder {
  File baseDir;
  Configuration conf;
  MiniDFSCluster hdfsCluster;
  public MiniDfsClusterBuilder(){
    conf = new Configuration();
    hdfsCluster = null;
  }
  
  /**
   * Creates a Mini HDFS cluster with default values of "test" and 3
   * @return String address to connect to MiniDFSCluster
   */
  public String build(){
    return this.build("test",3);
  }
  
  /**
   * Creates Mini HDFS cluster
   * @param testName Path of root file to write to
   * @param numNodes number of nodes to emulate
   * @return Link to connect to
   */
  public String build(String testName, int numNodes){
    baseDir = new File("./hdfs/" + testName).getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    conf.set("dfs.support.append", "true");
    
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    try {
      builder.numDataNodes(numNodes);
      hdfsCluster = builder.build();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
  }
  
  /**
   * Shuts down server and deletes baseDir
   */
  public void destroy(){
    hdfsCluster.shutdown();
    FileUtil.fullyDelete(baseDir);
  }
}
