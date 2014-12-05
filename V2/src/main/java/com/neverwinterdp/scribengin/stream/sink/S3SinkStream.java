package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class S3SinkStream implements SinkStream {

  private AmazonS3 s3;
  private String bucketName;
  private SinkPartitioner partititioner;
  private LinkedList<Tuple> buffer;
  private String name;
  private int maxBufferSize;
  private int maxBufferingTime;
  private long maxTupplesNumber;
  private SinkListner maxTuplesNumberListner;
  private SinkListner maxBufferSizeListner;
  private SinkListner maxBufferingTimeListner;
  private long count = 0;
  private long bufferSize;
  private long starTime = 0;
  private DB db;
  private ConcurrentNavigableMap<String, byte[]> map;


  public S3SinkStream(String bucketName, Regions regionName, long maxTupplesNumber, int maxBufferSize,
      int maxBufferingTime) {

    this.maxTupplesNumber = maxTupplesNumber;
    this.maxBufferSize = maxBufferSize;
    this.maxBufferingTime = maxBufferingTime *1000;
    File f = new File("tuplesFile");
    f.delete();
    db = DBMaker.newFileDB(new File("tuplesFile")).closeOnJvmShutdown().make();
    map = db.getTreeMap("tuplesMap");
    this.bucketName = bucketName;
    buffer = new LinkedList<Tuple>();
    AWSCredentials credentials = null;
    try {
      credentials = new ProfileCredentialsProvider().getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
    }

    s3 = new AmazonS3Client(credentials);
    Region region = Region.getRegion(regionName);
    s3.setRegion(region);
  }

  @Override
  public boolean prepareCommit() {
    // check if bucket exist
    if (!s3.doesBucketExist(bucketName)) {
      s3.createBucket(bucketName);
    }
    return true;
  }

  @Override
  public boolean commit() {
    if (buffer.size() > 0)
      bufferToDisk();
    File file;
    try {
      Iterator it = map.entrySet().iterator();
      String path;
      Writer writer;
      byte[] data;
      while (it.hasNext()) {
        Map.Entry entry = (Map.Entry) it.next();
        path = (String) entry.getKey();
        data = (byte[]) entry.getValue();
        file = File.createTempFile(path, ".log");
        String key = path.substring(path.lastIndexOf("/") + 1, path.length());
        String folder = path.substring(0, path.lastIndexOf("/"));
        writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write(data.toString());
        writer.flush();
        System.out.println("Uploading a new object to S3 from a file\n");
        PutObjectRequest object = new PutObjectRequest(folder, key, file);
        try {
          // upload to S3
          s3.putObject(object);
        } catch (AmazonServiceException ase) {
          System.out.println("Caught an AmazonServiceException, which means your request made it ");
        } catch (AmazonClientException ace) {
          System.out.println("Caught an AmazonClientException, which means the client encountered .");

        }
        writer.close();
      }

    } catch (IOException e) {
      // TODO Handle IOException
    }
    return true;
  }

  @Override
  public boolean clearBuffer() {
    buffer.clear();
    return true;
  }

  @Override
  public boolean completeCommit() {
    buffer.clear();
    return true;
  }

  @Override
  public boolean bufferTuple(Tuple tuple) {

    if (starTime == 0) {
      starTime = System.currentTimeMillis();
    }
    buffer.add(tuple);
    count++;
    bufferSize += tuple.getData().length;
    if (maxTuplesNumberListner != null && count >= maxTupplesNumber) {
      maxTuplesNumberListner.run();
    }
    if ( maxBufferSizeListner != null && bufferSize > maxBufferSize ) {
      maxBufferSizeListner.run();
    }
    if (maxBufferingTimeListner != null && System.currentTimeMillis() - starTime > maxBufferingTime) {
      maxBufferingTimeListner.run();
    }
    return true;

  }

  public void addOnMaxTuplesNumberListner(SinkListner maxTuplesNumberListner) {
    this.maxTuplesNumberListner = maxTuplesNumberListner;
  }

  public void addOnMaxBufferSizeListner(SinkListner maxBufferSizeListner) {
    this.maxBufferSizeListner = maxBufferSizeListner;
  }

  public void addOnMaxBufferingTimeListner(SinkListner maxBufferingTimeListner) {
    this.maxBufferingTimeListner = maxBufferingTimeListner;
  }

  public void bufferToDisk() {
    String path;
    for (Tuple t : buffer) {
      path = partititioner.getPartition(t.getCommitLogEntry().getStartOffset());
      map.put(path, t.getData());
      System.out.println(map.size());
    }

    db.commit();
    buffer.clear();
    count = 0;

  }

  @Override
  public boolean rollBack() {
    return true;
  }

  @Override
  public void setSinkPartitioner(SinkPartitioner sp) {
    this.partititioner = sp;

  }

  @Override
  public long getBufferSize() {
    return this.buffer.size() + map.size();
  }

  @Override
  public String getName() {
    return name;
  }

}
