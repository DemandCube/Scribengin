package com.neverwinterdp.scribengin.sink.s3;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.sink.ri.SinkStreamWriterImpl;

public class S3SinkStreamImpl implements SinkStream {
  AmazonS3 s3;

  Partitioner partitionner;
  public S3SinkStreamImpl(AmazonS3 s3, Partitioner partitionner) {
    this.s3 = s3;
    this.partitionner = partitionner;
  }

  @Override
  public int getId() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public SinkStreamWriter getWriter() {
    return new S3SinkStreamWriterImpl(s3, partitionner);
  }

}
