package com.neverwinterdp.vm.environment.yarn;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.neverwinterdp.vm.VMConfig;

public class VMResources extends HashMap<String, LocalResource> {
  public VMResources(Configuration conf, VMConfig vmConfig) throws FileNotFoundException, IllegalArgumentException, IOException {
    this(FileSystem.get(conf), vmConfig) ;
  }
  
  public VMResources(FileSystem fs, VMConfig vmConfig) throws FileNotFoundException, IllegalArgumentException, IOException {
    for(Map.Entry<String, String> entry : vmConfig.getVmResources().entrySet()) {
      addDir(fs, entry.getValue()) ;
    }
  }
  
  void addDir(FileSystem fs, String path) throws FileNotFoundException, IllegalArgumentException, IOException {
    addDir(fs, new Path(path)) ;
  }
  
  void addDir(FileSystem fs, Path path) throws FileNotFoundException, IOException {
    if(!fs.exists(path)) return ;
    
    if(fs.isFile(path)) {
      FileStatus fstatus = fs.getFileStatus(path);
      addFile(fstatus);
    } else {
      RemoteIterator<LocatedFileStatus> itr = fs.listFiles(path, true) ;
      while(itr.hasNext()) {
        FileStatus fstatus = itr.next() ;
        if(fstatus.isFile()) {
          addFile(fstatus);
        } else {
          addDir(fs, fstatus.getPath() );
        }
      }
    }
  }
  
  public void addFile(FileStatus fstatus) {
    Path fpath = fstatus.getPath() ;
    LocalResource libJar = Records.newRecord(LocalResource.class);
    libJar.setResource(ConverterUtils.getYarnUrlFromPath(fpath));
    libJar.setSize(fstatus.getLen());
    libJar.setTimestamp(fstatus.getModificationTime());
    libJar.setType(LocalResourceType.FILE);
    libJar.setVisibility(LocalResourceVisibility.PUBLIC);
    put(fpath.getName(), libJar) ;
  }
}