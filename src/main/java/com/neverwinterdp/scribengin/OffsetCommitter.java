package com.neverwinterdp.scribengin;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OffsetCommitter {
  private FSDataOutputStream os;
  private FileSystem fs;

  public OffsetCommitter(String uri) throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

    fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);

    if (fs.exists(path)) {
      System.out.println("File " + uri + " already exists");
      os = fs.append(path);
    } else {
      System.out.println("File " + uri + " not found. So create one");
      os = fs.create(path);
    }
  }

  public void commitOffset(long offset) throws IOException {
    os.writeLong(offset);
    os.writeChars("\n");
  }

  public void close() {
    try {
      os.close();
    } catch (IOException e) {
      //TODO: log
    }

    try {
      fs.close();
    } catch (IOException e) {
      // TODO: log
    }
  }

}
