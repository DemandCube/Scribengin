package com.neverwinterdp.scribengin.writer.helpers;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Singleton class
 */
public class StringRecordWriter {
  private static volatile StringRecordWriter singleton = null;
  private static FSDataOutputStream os;
  private static FileSystem fs;

  public static StringRecordWriter getInstance(String uri){
    if(singleton==null){
      //Thread safe
      synchronized (StringRecordWriter.class) {
        // Double check
        if (singleton == null) {
          singleton = new StringRecordWriter(uri);
        }
      }
    }
    return singleton;
  }
  
  
  private StringRecordWriter(String uri) {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

    try {
      fs = FileSystem.get(URI.create(uri), conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    Path path = new Path(uri);

    try {
      if (fs.exists(path)) {
        os = fs.append(path);
      } else {
        os = fs.create(path);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void write(byte[] bytes) throws IOException {
    os.write(bytes);
    os.write('\n');
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
