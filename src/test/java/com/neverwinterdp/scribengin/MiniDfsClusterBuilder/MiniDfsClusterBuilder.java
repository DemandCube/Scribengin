package com.neverwinterdp.scribengin.MiniDfsClusterBuilder;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class MiniDfsClusterBuilder {
  File baseDir;
  Configuration conf;
  MiniDFSCluster hdfsCluster;
  public MiniDfsClusterBuilder(){
    conf = new Configuration();
    hdfsCluster = null;
  }
  
  public String build(){
    return this.build("test");
  }
  
  /**
   * @param testName Path of file to write to
   * @return Link to connect to
   */
  public String build(String testName){
    baseDir = new File("./hdfs/" + testName).getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    try {
      hdfsCluster = builder.build();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
  }
  
  public void destroy(){
    hdfsCluster.shutdown();
    FileUtil.fullyDelete(baseDir);
  }
}
