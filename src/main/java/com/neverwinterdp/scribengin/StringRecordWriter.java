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
    Path path = new Path(uri); //TODO: hardcode

    //boolean flag = Boolean.getBoolean(fs.getConf().get("dfs.support.append"));
    //System.out.println("dfs.support.append is set to: " + flag); //xxx

    if (fs.exists(path)) {
      System.out.println("File " + uri + " already exists");
      os = fs.append(path);
      PrintWriter writer = new PrintWriter(os);
      writer.append("appended content\n");
      writer.close();
      fs.close();
    } else {
      System.out.println("File " + uri + " not found. So create one");
      os = fs.create(path);
      os.writeChars("hello world2\n");
      os.close();
      fs.close();
    }

    // just write to it and see what happens

    //try {
      //System.out.println(" ?>>>>>>>>>>>> " + uri);
      //fs = FileSystem.get( URI.create(uri), conf );
      //System.out.println("here0");
    //} catch (Exception e) {
      ////e.printStackTrace();
      //try {
        //fs = FileSystem.get( URI.create("/tmp/scribe_data"), conf );
        //System.out.println("here1");
      //} catch (Exception e1) {
        ////e1.printStackTrace();
        //fs = FileSystem.get( URI.create("hdfs://localhost:9092"), conf );
        //System.out.println("here2");
      //}
    //}

    //os = fs.append(new Path(uri));
  }

  public void write(byte[] bytes) throws IOException {
    //os.write(bytes);
    //br.write(bytes, 0, bytes.length);
  }

  public void close() {
    try {
      os.close();
    } catch (IOException e) {
    }

    try {
      fs.close();
    } catch (IOException e) {
    }
  }
}
