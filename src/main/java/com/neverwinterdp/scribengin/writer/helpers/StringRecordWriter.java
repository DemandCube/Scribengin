package com.neverwinterdp.scribengin.writer.helpers;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.ScribenginContext;


public class StringRecordWriter {
  private FSDataOutputStream os;
  private FileSystem fs;
  private Configuration conf;
  private String hdfsPath;
  
  public StringRecordWriter(String hdfsPath, String[] resources) {
    this.hdfsPath = hdfsPath;
    //If these files don't exist, no error occurs.
    //These will just be the default for if this is 
    //running on the hadoop master
    conf = new Configuration();
    for(int i=0; i<resources.length; i++){
      conf.addResource(resources[i]);
    }
  }
  
  public StringRecordWriter(String hdfsPath){
    this(hdfsPath, new String[]{"/etc/hadoop/conf/hdfs-site.xml", 
                  "/etc/hadoop/conf/core-site.xml"});
    
  }

  public void write(byte[] bytes) throws IOException {
    fs = FileSystem.get(URI.create(this.hdfsPath), conf);
    Path path = new Path(this.hdfsPath);
    
    if (fs.exists(path)) {
      os = fs.append(path);
    } else {
      os = fs.create(path);
    }
    os.write(bytes);
    os.flush();
    //os.write('\n');
  }

  public void close() {
    try {
      os.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
