package com.neverwinterdp.scribengin.filesystem;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

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
    FileSystem fs = FileSystem.get(conf);
    return fs;
  }

  @Override
  public FileSystem build(URI uri) throws IOException {
    return this.build();
  }
}
