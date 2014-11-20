package com.neverwinterdp.scribengin.sink.s3;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class S3FileSystemUnitTest {
  private String protocol           = "s3n";
  private String testFileName       = "test.txt";

  // Please update awsAccessKeyId when running this test
  private String awsAccessKeyId     = "xxxx";

  // Please update awsSecretAccessKey when running this test
  private String awsSecretAccessKey = "xxxx";

  private String s3Bucket           = "pleslie";
  private String s3Path             = "s3test";

  @Test
  public void testUsingHadoopFileSystemS3Write() throws Exception {
    Assert.assertFalse("Please update the awsAccessKeyId in this unit test",
        "xxxx".equals(this.awsAccessKeyId));
    Assert.assertFalse("Please update the awsSecretAccessKey in this unit test",
        "xxxx".equals(this.awsSecretAccessKey));

    Path srcPath = new Path("./build/s3/" + testFileName);
    String destPathStr = protocol + "://" + s3Bucket + "/" + s3Path + "/" + testFileName;
    Path destPath = new Path(destPathStr);

    createLocalData(srcPath);

    getFileSystem(destPathStr).moveFromLocalFile(srcPath, destPath);
    Thread.sleep(1000);
    String path = getS3Paths(destPathStr)[0];

    Assert.assertEquals(path, destPathStr);
  }

  @Test
  public void testUsingHadoopFileSystemS3List() throws IOException {
    Assert.assertFalse("Please update the awsAccessKeyId in this unit test",
        "xxxx".equals(this.awsAccessKeyId));
    Assert.assertFalse("Please update the awsSecretAccessKey in this unit test",
        "xxxx".equals(this.awsSecretAccessKey));
    String path = protocol + "://" + s3Bucket + "/" + s3Path + "/";
    getS3Paths(path);
  }

  private String[] getS3Paths(String path) throws IOException {
    FileSystem fs = getFileSystem(path);
    Path fsPath = new Path(path);
    ArrayList<String> paths = new ArrayList<String>();
    FileStatus[] statuses = fs.listStatus(fsPath);
    if (statuses != null) {
      for (FileStatus status : statuses) {
        Path statusPath = status.getPath();
        System.out.println(statusPath.toUri().toString());
        paths.add(statusPath.toUri().toString());
      }
    }
    return paths.toArray(new String[] {});
  }

  private void createLocalData(Path path) throws IOException {
    FileSystem localfs;
    String TEXT = "hello s3 test";
    localfs = FileSystem.getLocal(new Configuration());
    FSDataOutputStream os = localfs.create(path);
    os.write(TEXT.getBytes());
    os.close();
    localfs.close();

  }

  private FileSystem getFileSystem(String path) throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs." + protocol + ".awsAccessKeyId", this.awsAccessKeyId);
    conf.set("fs." + protocol + ".awsSecretAccessKey", this.awsSecretAccessKey);
    return FileSystem.get(URI.create(path), conf);
  }
}
