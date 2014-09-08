package com.neverwinterdp.scribengin.writer.helpers;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// TODO singleton it
public class StringRecordWriter {
  private FSDataOutputStream os;
  private FileSystem fs;
  private Configuration conf;

  public StringRecordWriter() throws IOException {
    this(new String[]{"/etc/hadoop/conf/hdfs-site.xml", 
                      "/etc/hadoop/conf/core-site.xml"});
  }
  
  public StringRecordWriter(String[] resources){
    conf = new Configuration();
    for(int i=0; i<resources.length; i++){
      conf.addResource(resources[i]);
    }
  }

  public void write(String uri, byte[] bytes) throws IOException {
    fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);
    
    if (fs.exists(path)) {
      os = fs.append(path);
    } else {
      os = fs.create(path);
    }
    
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
