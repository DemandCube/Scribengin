package com.neverwinterdp.scribengin;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StringRecordWriter {
  private FSDataOutputStream os;
  private FileSystem fs;
  private BufferedWriter br;

  public StringRecordWriter(String uri) throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

    fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);

    //boolean flag = Boolean.getBoolean(fs.getConf().get("dfs.support.append"));
    //System.out.println("dfs.support.append is set to: " + flag);

    if (fs.exists(path)) {
      System.out.println("File " + uri + " already exists");
      os = fs.append(path);
      //os.writeChars("appended content\n");
      //os.close();
      //fs.close();
    } else {
      System.out.println("File " + uri + " not found. So create one");
      os = fs.create(path);
      //os.writeChars("hello world2\n");
      //os.close();
      //fs.close();
    }
  }

  public void write(byte[] bytes) throws IOException {
    os.writeChars(new String(bytes).concat("\n"));
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
