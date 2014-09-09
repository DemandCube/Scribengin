package com.neverwinterdp.scribengin.writer.helpers;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Helper object to write to HDFS
 * Only will ever write to one file, as defined by the constructor
 * @author Richard Duarte
 *
 */
public class StringRecordWriter {
  private FSDataOutputStream os;
  private FileSystem fs;
  private Configuration conf;
  private String hdfsPath;
  
  /**
   * Constructor
   * @param hdfsPath Path of HDFS file to write to
   * @param resources Resource files to add to Configuration()
   */
  public StringRecordWriter(String hdfsPath, String[] resources) {
    this.hdfsPath = hdfsPath;
    
    conf = new Configuration();
    for(int i=0; i<resources.length; i++){
      conf.addResource(resources[i]);
    }
  }
  
  /**
   * Default constructor, sets default hadoop xml config files
   * @param hdfsPath Path of file to write to
   */
  public StringRecordWriter(String hdfsPath){
    //If these files don't exist, no error occurs.
    //These will just be the default for if this is 
    //running on the hadoop master
    this(hdfsPath, new String[]{"/etc/hadoop/conf/hdfs-site.xml", 
                  "/etc/hadoop/conf/core-site.xml"});
  }

  /**
   * Writes, flushes, and closes file handle to HDFS
   * @param bytes byte array to write to HDFS
   * @throws IOException
   */
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
    this.close();
  }

  /**
   * Closes connection
   */
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