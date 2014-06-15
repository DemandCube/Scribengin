package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;


public class ScribeCommitLog {
  private FileSystem fs;
  //private List recentLogEntryList;
  private ListIterator<ScribeLogEntry> recentLogIter;
  private Path path;

  public ScribeCommitLog(String uri) throws IOException
  {
    this(uri, false);
  }

  public ScribeCommitLog(String uri, boolean useLocalFS) throws IOException
  {
    Configuration conf;
    path = new Path(uri);

    if (useLocalFS == false) {
      // hdfs
      conf = new Configuration();
      conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
      conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
      fs = FileSystem.get(URI.create(uri), conf);
    } else {
      // local
      fs = new LocalFileSystem();
      System.out.println(fs);//xxx
      fs.initialize(URI.create(uri), new Configuration());
    }
  }

  public void record(long startOffset, long endOffset, String srcPath, String destPath)
      throws NoSuchAlgorithmException, IOException
  {
    FSDataOutputStream os;
    if (fs.exists(path)) {
      os = fs.append(path);
    } else {
      os = fs.create(path);
    }

    ScribeLogEntry entry = new ScribeLogEntry(startOffset, endOffset, srcPath, destPath);
    String jsonStr = ScribeLogEntry.toJson(entry);
    os.write(jsonStr.getBytes());
    os.write('\n');

    try {
      os.close();
    } catch (IOException e) {
      //TODO: log
    }
    fsClose();
  }

  public void readLastTwoEntries() throws IOException, NoSuchAlgorithmException
  {
    CircularFifoBuffer buffer = new CircularFifoBuffer(2);

    if (fs.exists(path)) {
      String currJsonStr;
      FSDataInputStream in = fs.open(path);

      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      while ((currJsonStr = br.readLine()) != null) {
        ScribeLogEntry e = ScribeLogEntry.fromJson(currJsonStr);
        buffer.add(e);
      }// while

      try{
        in.close();
      } catch(IOException e) {
        //TODO: log
      }
      fsClose();
    }

    List<ScribeLogEntry> recentLogEntryList = (List<ScribeLogEntry>)(List<?>)Arrays.asList(buffer.toArray());
    recentLogIter = recentLogEntryList.listIterator();
  }

  public ScribeLogEntry getLatestEntry() {
    ScribeLogEntry r = null;
    if (recentLogIter.hasPrevious()) {
      r = recentLogIter.previous();
    }
    return r;
  }

  public void fsClose()
  {
    try {
      fs.close();
    } catch (IOException e) {
      // TODO: log
    }
  }
}
