package com.neverwinterdp.scribengin.storage.source;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.scribengin.storage.kafka.source.KafkaSource;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;

@Singleton
public class SourceFactory {
  @Inject
  private FileSystem fs;
  @Inject
  private S3Client s3Client;

  public Source create(StorageDescriptor descriptor) throws Exception {
    if ("hdfs".equalsIgnoreCase(descriptor.getType())) {
      return new HDFSSource(fs, descriptor);
    } else if ("kafka".equalsIgnoreCase(descriptor.getType())) {
      return new KafkaSource(descriptor);
    } else if ("s3".equalsIgnoreCase(descriptor.getType())) {
      return new S3Source(s3Client, descriptor);
    }
    throw new Exception("Unknown source type " + descriptor.getType());
  }

  public Source create(StreamDescriptor descriptor) throws Exception {
    if ("hdfs".equalsIgnoreCase(descriptor.getType())) {
      return new HDFSSource(fs, descriptor);
    } else if ("kafka".equalsIgnoreCase(descriptor.getType())) {
      return new KafkaSource(descriptor);
    } else if ("s3".equalsIgnoreCase(descriptor.getType())) {
      System.out.println("descriptor location "+ descriptor.getLocation());
      return new S3Source(s3Client, descriptor);
    }
    throw new Exception("Unknown source type " + descriptor.getType());
  }
}
