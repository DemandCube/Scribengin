package com.neverwinterdp.scribengin.sink.s3;

import java.util.Arrays;
import java.util.LinkedHashMap;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkStream;

public class S3SinkImpl implements Sink {

  private LinkedHashMap<Integer, SinkStream> datastreams = new LinkedHashMap<Integer, SinkStream>() ;
  private AmazonS3 s3;
  Partitioner partitionner;
  int count;
  
  public S3SinkImpl(Partitioner partitionner) {
    AWSCredentials credentials = null;
    try {
      credentials = new ProfileCredentialsProvider().getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
          + "Please make sure that your credentials file is at the correct "
          + "location (~/.aws/credentials), and is in valid format.", e);
    }

    s3 = new AmazonS3Client(credentials);
    Region usWest2 = Region.getRegion(Regions.US_WEST_2);
    s3.setRegion(usWest2);
    this.partitionner = partitionner;
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public SinkStream[] getSinkStreams() {
    return Arrays.copyOf( datastreams.values().toArray(),  datastreams.values().toArray().length, SinkStream[].class);
  }

  @Override
  public void delete(SinkStream stream) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public SinkStream newSinkStream() throws Exception {

    S3SinkStreamImpl datastream = new S3SinkStreamImpl(s3, partitionner);
    datastreams.put(count++, datastream) ;
    return datastream;
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub

  }



}
