package com.neverwinterdp.scribengin.storage.s3;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Util {
  static public int getStreamId(String name) {
    int dashIdx = name.lastIndexOf('-');
    return Integer.parseInt(name.substring(dashIdx + 1));
  }

  public static void listObjects(S3Client client, String bucketName) {
    System.out.println("Listing objects in bucket " + bucketName);
    ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName);
    ObjectListing objectListing = client.getAmazonS3Client().listObjects(request);
    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
      System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
    }
  }
}
