package com.neverwinterdp.scribengin;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

import com.neverwinterdp.scribengin.AbstractFileSystemFactory;

public class UnitTestCluster extends AbstractFileSystemFactory {
  private static UnitTestCluster inst = null;
  private MiniDFSCluster hdfsCluster = null;
  private String clusterPath = null;
  private UnitTestCluster (String clusterPath) {
    this.clusterPath = clusterPath;
  }

  public static UnitTestCluster instance(String clusterPath) {
    if (inst == null)
      inst = new UnitTestCluster(clusterPath);
    return inst;
  }

  public FileSystem build() throws IOException {
    MiniDFSCluster miniCluster = createMiniDFSCluster(clusterPath, 1);
    FileSystem r = miniCluster.getFileSystem();
    return r;
  }

  protected MiniYARNCluster createMiniYARNCluster(int numOfNodeManagers) throws Exception {
    return createMiniYARNCluster(new YarnConfiguration(), numOfNodeManagers) ;
  }

  protected MiniYARNCluster createMiniYARNCluster(Configuration yarnConf, int numOfNodeManagers) throws Exception {
    yarnConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    yarnConf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
    MiniYARNCluster miniYarnCluster = new MiniYARNCluster("yarn", numOfNodeManagers, 1, 1);
    miniYarnCluster.init(yarnConf);
    yarnConf.set("yarn.resourcemanager.scheduler.address", "0.0.0.0:8030") ;
    miniYarnCluster.start();
    return miniYarnCluster ;
  }

  private MiniDFSCluster createMiniDFSCluster(String dir, int numDataNodes) throws IOException {
    return createMiniDFSCluster(new Configuration(), dir, numDataNodes) ;
  }

  private MiniDFSCluster createMiniDFSCluster(Configuration conf, String dir, int numDataNodes) throws IOException {
    if (this.hdfsCluster == null) {
      File baseDir = new File(dir).getAbsoluteFile();
      FileUtil.fullyDelete(baseDir);
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
      MiniDFSCluster _hdfsCluster =
        new MiniDFSCluster.Builder(conf).
        numDataNodes(numDataNodes).
        build();
      _hdfsCluster.waitClusterUp();
      this.hdfsCluster = _hdfsCluster;
    }
    return this.hdfsCluster;
  }

  @Override
  public FileSystem build(URI uri) throws IOException {
    return this.build();
  }
}

