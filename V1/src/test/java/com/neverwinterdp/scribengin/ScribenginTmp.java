package com.neverwinterdp.scribengin;
 
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.UUID;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 
public class ScribenginTmp {
 
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    
//    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
//    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
//    conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
//    conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
    String hostname="";
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    
    String uri = "hdfs://"+hostname+":8020";
    System.out.println(uri);
    
    
    System.out.println("STARTING");
    String filepath = uri+"/user/yarn/"+UUID.randomUUID().toString();
    System.out.println(filepath);
    Path path = new Path(filepath);
    
    System.out.println("FIRST WRITE");
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    FSDataOutputStream os = fs.create(path);
    os.write("hellothere\n".getBytes());
    os.close();
    
    System.out.println("APPEND");
    FileSystem fs2 = FileSystem.get(URI.create(uri), conf);
    System.out.println("GETTING OUTPUT STREAM");
    FSDataOutputStream os2 = fs2.append(path);
    System.out.println("DOING THE SECOND WRITE");
    os2.write("Hope this works\n".getBytes());
    System.out.println("CLOSING SECOND WRITE");
    os2.close();
    System.out.println("DONE");
    
  }
 
}