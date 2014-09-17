package com.neverwinterdp.scribengin.filesystem;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

public abstract class AbstractFileSystemFactory {
  public abstract FileSystem build() throws IOException;
  public abstract FileSystem build(URI uri) throws IOException;
}
