package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OffsetReader {
  private FSDataInputStream in;
  private FileSystem fs;

  public OffsetReader(String uri) throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

    fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);

    if (fs.exists(path)) {
      System.out.println("File " + uri + " already exists");
      in = fs.open(path);
    } else {
      System.out.println("File " + uri + " not found. So create one");
    }
  }

  public long readLatestOffset() throws IOException {
    long r = -1;
    if (in != null) {
      String latestOffsetStr;
      BufferedReader br = new BufferedReader(new InputStreamReader(in));

      while ((latestOffsetStr = br.readLine()) != null) {
        r = Long.valueOf(latestOffsetStr).longValue();
      }
    }
    return r;
  }

  public void close() {
    try {
      if (in != null) {
        in.close();
      }
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
