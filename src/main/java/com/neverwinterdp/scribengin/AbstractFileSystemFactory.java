package com.neverwinterdp.scribengin;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

public abstract class AbstractFileSystemFactory {
  public abstract FileSystem build() throws IOException;
}
