package com.neverwinterdp.vm.environment.yarn;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

public class MiniClusterUtil {
  
  static public MiniYARNCluster createMiniYARNCluster(int numOfNodeManagers) throws Exception {
    YarnConfiguration conf = new YarnConfiguration() ;
    MiniYARNCluster cluster = createMiniYARNCluster(conf, numOfNodeManagers) ;
    return cluster ;
  }
  
  static public MiniYARNCluster createMiniYARNCluster(Configuration yarnConf, int numOfNodeManagers) throws Exception {
    yarnConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    yarnConf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
    MiniYARNCluster miniYarnCluster = new MiniYARNCluster("yarn", numOfNodeManagers, 1, 1);
    miniYarnCluster.init(yarnConf);
    yarnConf.set("yarn.resourcemanager.scheduler.address", "0.0.0.0:8030") ;
    miniYarnCluster.start();
    //wait to make sure the server is started
    //TODO: find a way to fix this
    Thread.sleep(3000);
    return miniYarnCluster ;
  }
  
  static public MiniDFSCluster createMiniDFSCluster(String dir, int numDataNodes) throws Exception {
    return createMiniDFSCluster(new Configuration(), dir, numDataNodes) ;
  }
  
  static public MiniDFSCluster createMiniDFSCluster(Configuration conf, String dir, int numDataNodes) throws Exception {
    File baseDir = new File(dir).getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster hdfsCluster =
        new MiniDFSCluster.Builder(conf).
        nnTopology(MiniDFSNNTopology.simpleSingleNN(8020, 50070)).
        numDataNodes(numDataNodes).
        build();
    hdfsCluster.waitClusterUp();
    String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
    System.out.println("hdfs uri: " + hdfsURI) ;
    FileSystem fs = hdfsCluster.getFileSystem();
    //final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    //dfs.copyFromLocalFile(false, false, new Path("target/hadoop-samples-1.0.jar"), new Path("/tmp/hadoop-samples-1.0.jar"));
    return hdfsCluster ;
  }
}