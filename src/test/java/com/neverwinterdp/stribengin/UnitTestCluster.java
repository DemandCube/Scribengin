package com.neverwinterdp.stribengin;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

public class UnitTestCluster {
  private static MiniDFSCluster hdfsCluster;

  protected static MiniYARNCluster createMiniYARNCluster(int numOfNodeManagers) throws Exception {
    return createMiniYARNCluster(new YarnConfiguration(), numOfNodeManagers) ;
  }

  protected static MiniYARNCluster createMiniYARNCluster(Configuration yarnConf, int numOfNodeManagers) throws Exception {
    yarnConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    yarnConf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
    MiniYARNCluster miniYarnCluster = new MiniYARNCluster("yarn", numOfNodeManagers, 1, 1);
    miniYarnCluster.init(yarnConf);
    yarnConf.set("yarn.resourcemanager.scheduler.address", "0.0.0.0:8030") ;
    miniYarnCluster.start();
    return miniYarnCluster ;
  }

  protected static MiniDFSCluster createMiniDFSCluster(String dir, int numDataNodes) throws IOException {
    return createMiniDFSCluster(new Configuration(), dir, numDataNodes) ;
  }

  protected static MiniDFSCluster createMiniDFSCluster(Configuration conf, String dir, int numDataNodes) throws IOException {
    if (UnitTestCluster.hdfsCluster == null) {
      File baseDir = new File(dir).getAbsoluteFile();
      FileUtil.fullyDelete(baseDir);
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
      MiniDFSCluster hdfsCluster =
        new MiniDFSCluster.Builder(conf).
        numDataNodes(numDataNodes).
        build();
      hdfsCluster.waitClusterUp();
      //String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
      //System.out.println("hdfs uri: " + hdfsURI) ;
      //FileSystem fs = hdfsCluster.getFileSystem();
      //Assert.assertTrue("Not a HDFS: "+ fs.getUri(), fs instanceof DistributedFileSystem);
      //final DistributedFileSystem dfs = (DistributedFileSystem)fs;
      //dfs.copyFromLocalFile(false, false, new Path("target/hadoop-samples-1.0.jar"), new Path("/tmp/hadoop-samples-1.0.jar"));
      UnitTestCluster.hdfsCluster = hdfsCluster;
    }
    return UnitTestCluster.hdfsCluster;
  }
}

