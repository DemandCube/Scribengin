package com.neverwinterdp.scribengin.filesystem;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemFactory extends AbstractFileSystemFactory {
  private static FileSystemFactory inst = null;

  public static FileSystemFactory instance() {
    if (inst == null)
      inst = new FileSystemFactory();
    return inst;
  }

  public FileSystem build() throws IOException
  {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    FileSystem fs = FileSystem.get(conf);
    return fs;
  }
  

  public FileSystem build(URI uri) throws IOException {
    return this.build();
  }
}
