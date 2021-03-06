package com.neverwinterdp.scribengin.commitlog;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


public class ScribeCommitLog {
  private static final int NUM_CORRECT_RECORDS = 3;
  private static final Logger log =
      Logger.getLogger(ScribeCommitLog.class);

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

    //if (useLocalFS == false) {
      // hdfs
      conf = new Configuration();
      conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
      conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
      conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
      conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
      
      fs = FileSystem.get(URI.create(uri), conf);
    //} else {
      // local
      // It's unit test's responsiblility to set fs.
    //}
  }

  public void record(long startOffset, long endOffset, String srcPath, String destPath)
      throws NoSuchAlgorithmException, IOException
  {
    FSDataOutputStream os = null;
    //appending doesn't work
    //So read in old file
    //Write out new file
    if (fs.exists(path)) {
      //this append nonsense doesn't work
      //os = fs.append(path);
      
      //read in the old log
      BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
      String finalLine="";
      String line=br.readLine();
      finalLine += line;
      while (line != null){
        line = br.readLine();
        finalLine += line;
      }
      
      os = fs.create(path);
      if(finalLine != null && !finalLine.isEmpty()){
        os.write(finalLine.getBytes());
        
        ScribeLogEntry entry = new ScribeLogEntry(startOffset, endOffset, srcPath, destPath);
        String jsonStr = ScribeLogEntry.toJson(entry);
        os.write(jsonStr.getBytes());
        os.write('\n');
        
      }
    }
    //Create the new file
    else {
      os = fs.create(path);
      ScribeLogEntry entry = new ScribeLogEntry(startOffset, endOffset, srcPath, destPath);
      String jsonStr = ScribeLogEntry.toJson(entry);
      os.write(jsonStr.getBytes());
      os.write('\n');
    }

    //close
    if(os != null){
      try {
        os.close();
      } catch (IOException e) {
        log.error(e.getMessage());
        e.printStackTrace();
      }
      //fsClose();
    }
  }

  public void read() throws IOException
  {
    ArrayList<ScribeLogEntry> logEntryList = new ArrayList<ScribeLogEntry>();

    if (fs.exists(path)) {
      FileStatus status = fs.getFileStatus(path);
      long fptr = status.getLen() - 1;

      FSDataInputStream in = fs.open(path);
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();


      int correctEntryCnt = 0;
      System.out.println(">>fptr : " + fptr); //xxx
      while (fptr >= 0) {
        in.seek(fptr);
        byte b = in.readByte();
        if (b != '\n') {
          buffer.write(b);
        }

        if (b == '\n' || fptr == 0) {
          if (buffer.size() > 0) {
            // read from the back of the file, so we'll have to reverse the string.
            String jsonStr = new StringBuffer(buffer.toString()).reverse().toString();
            buffer.reset();
            //TODO: log the jsonStr's content
            System.out.println(">> jsonStr: " + jsonStr); //xxx

            ScribeLogEntry e = ScribeLogEntry.fromJson(jsonStr);

            logEntryList.add(e);

            try {
              if (e.isCheckSumValid()) {
                correctEntryCnt++;
              }
            } catch (NoSuchAlgorithmException ex) {
              //TODO: log
            }

            if (correctEntryCnt == NUM_CORRECT_RECORDS) {
              break;
            }
          }
        }
        fptr--;
      } //while

      // Check to see if the last character is '\n'
      // If not, make sure to write an extra '\n' to the log file
      in.seek(status.getLen() - 1);
      if (in.readByte() != '\n') {
        FSDataOutputStream os = fs.append(path);
        os.write('\n');
        try {
          os.close();
        } catch (IOException e) {
          //TODO: log
        }
      }

      //fsClose();
    }

    Collections.reverse(logEntryList);
    recentLogIter = logEntryList.listIterator(logEntryList.size());
  }

  // return null, if there's no more entry in the log
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
      log.error(e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Clear the contents of the commit log.
   * 
   * */
  public void clear() throws IOException {
    // fs.truncate(path);
    System.out.println("DELETING LOG "+path.toString());
    fs.delete(path, true);
    while(fs.exists(path)){}
    System.out.println("DELETING LOG COMPLETE");
    //fsClose();
  }
}
