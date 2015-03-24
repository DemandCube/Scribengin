package com.neverwinterdp.scribengin.dataflow.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.storage.hdfs.HDFSSourceGenerator;


public class HdfsDataflowTest extends DataflowTest {

  protected void doRun(ScribenginShell shell) throws Exception {
    ScribenginShell scribenginShell = (ScribenginShell) shell;
    FileSystem fs = FileSystem.getLocal(new Configuration());
    HelloHDFSDataflowBuilder hdfDataflowBuilder = 
      new HelloHDFSDataflowBuilder(scribenginShell.getScribenginClient(), getDataDir());
    new HDFSSourceGenerator().generateSource(fs, getDataDir() + "/source");
    ScribenginWaitingEventListener eventListener = hdfDataflowBuilder.submit();
    eventListener.waitForEvents(60000);
  }

  private String getDataDir() { return "./build/hdfs"; }
}