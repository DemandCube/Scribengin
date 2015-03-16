package com.neverwinterdp.scribengin.s3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using the
 * AWS SDK for Java.
 * 
 * Prerequisites: You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on Amazon
 * S3, see http://aws.amazon.com/s3.
 * 
 * Important: Be sure to fill in your AWS access credentials in
 * ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users)
 * before you try to run this sample.
 * 
 */
public class S3FeaturesDemoTest {
  private AmazonS3 s3Client;

  @Test
  public void runFeaturesDemo() throws Exception {
    System.out.println("===========================================");
    System.out.println("Getting Started with Amazon S3");
    System.out.println("===========================================\n");

    try {
      String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();
      String key = "MyObjectKey";
      init();
      createBucket(bucketName);
      listBuckets();
      
      upload(bucketName, key, "this is a tets".getBytes(), "text/plain");
      multipartUpload(bucketName, "multipart-uppload", "build.gradle");
      
      S3Object object = download(bucketName, key);
      displayTextInputStream(object.getObjectContent());
      
      listingObjects(bucketName, key);
      
      deleteObject(bucketName, key);
      deleteObject(bucketName, "multipart-uppload");
      
      deleteBucket(bucketName);
    } catch (AmazonServiceException ase) {
      System.out.println("Caught an AmazonServiceException, which means your request made it "
          + "to Amazon S3, but was rejected with an error response for some reason.");
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      System.out.println("Caught an AmazonClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with S3, "
          + "such as not being able to access the network.");
      System.out.println("Error Message: " + ace.getMessage());
    }
  }
  
  public void init() throws Exception {
    /*
     * Create your credentials file at ~/.aws/credentials
     * (C:\Users\USER_NAME\.aws\credentials for Windows users) and save the
     * following lines after replacing the underlined values with your own.
     * 
     * [default] aws_access_key_id = YOUR_ACCESS_KEY_ID aws_secret_access_key =
     * YOUR_SECRET_ACCESS_KEY
     */
    s3Client = new AmazonS3Client();
    Region region = Region.getRegion(Regions.US_WEST_2);
    //Region region = Region.getRegion(Regions.SA_EAST_1);
    s3Client.setRegion(region);
  }

  public void createBucket(String bucketName) throws AmazonServiceException {
    /*
     * Create a new S3 bucket - Amazon S3 bucket names are globally unique, so
     * once a bucket name has been taken by any user, you can't create another
     * bucket with that same name.
     * 
     * You can optionally specify a location for your bucket if you want to
     * keep your data closer to your applications or users.
     */
    System.out.println("Creating bucket " + bucketName + "\n");
    s3Client.createBucket(bucketName);
  }

  public void listBuckets() throws AmazonServiceException {
    System.out.println("Listing buckets: ");
    for (Bucket bucket : s3Client.listBuckets()) {
      System.out.println(" - " + bucket.getName());
    }
  }

  public void upload(String bucketName, String key, byte[] data, String mimeType) throws AmazonServiceException {
    /*
     * Upload an object to your bucket - You can easily upload a file to S3,
     * or upload directly an InputStream if you know the length of the data in
     * the stream. You can also specify your own metadata when uploading to
     * S3, which allows you set a variety of options like content-type and
     * content-encoding, plus additional metadata specific to your
     * applications.
     */
    System.out.println("Uploading a new object to S3\n");
    InputStream is = new ByteArrayInputStream(data);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(mimeType);
    s3Client.putObject(new PutObjectRequest(bucketName, key, is, metadata));
  }

  public S3Object download(String bucketName, String key) throws AmazonServiceException {
    /*
     * Download an object - When you download an object, you get all of the
     * object's metadata and a stream from which to read the contents. It's
     * important to read the contents of the stream as quickly as possibly
     * since the data is streamed directly from Amazon S3 and your network
     * connection will remain open until you read all the data or close the
     * input stream.
     * 
     * GetObjectRequest also supports several other options, including
     * conditional downloading of objects based on modification times, ETags,
     * and selectively downloading a range of an object.
     */
    System.out.println("Downloading an object");
    S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
    System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
    return object;
  }

  public void listingObjects(String bucketName, String key) throws AmazonServiceException {
    /**
     * List objects in your bucket by prefix - There are many options for
     * listing the objects in your bucket. Keep in mind that buckets with many
     * objects might truncate their results when listing their objects, so be
     * sure to check if the returned object listing is truncated, and use the
     * AmazonS3.listNextBatchOfObjects(...) operation to retrieve additional
     * results.
     */
    System.out.println("Listing objects");
    ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName).withPrefix("My");
    ObjectListing objectListing = s3Client.listObjects(request);
    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
      System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
    }
    System.out.println();
  }

  public void deleteObject(String bucketName, String key) throws AmazonServiceException {
    /*
     * Delete an object - Unless versioning has been turned on for your
     * bucket, there is no way to undelete an object, so use caution when
     * deleting objects.
     */
    System.out.println("Deleting the object " + key + " in bucket " + bucketName);
    s3Client.deleteObject(bucketName, key);
  }

  public void deleteBucket(String bucketName) throws AmazonServiceException {
    /**
     * Delete a bucket - A bucket must be completely empty before it can be
     * deleted, so remember to delete any objects from your buckets before you
     * try to delete them.
     */
    System.out.println("Deleting bucket " + bucketName + "\n");
    s3Client.deleteBucket(bucketName);
  }
  
  public void multipartUpload(String bucketName, String keyName, String filePath) throws IOException {
    //Create a list of UploadPartResponse objects. You get one of these for each part upload.
    List<PartETag> partETags = new ArrayList<PartETag>();

    // Step 1: Initialize.
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

    File file = new File(filePath);
    long contentLength = file.length();
    long partSize = 5 * 1024 * 1024; // Set part size to 1kb.

    try {
      // Step 2: Upload parts.
      long filePosition = 0;
      for (int i = 1; filePosition < contentLength; i++) {
        // Last part can be less than 5 MB. Adjust part size.
        partSize = Math.min(partSize, (contentLength - filePosition));

        // Create request to upload a part.
        UploadPartRequest uploadRequest = new UploadPartRequest()
            .withBucketName(bucketName).withKey(keyName)
            .withUploadId(initResponse.getUploadId()).withPartNumber(i)
            .withFileOffset(filePosition)
            .withFile(file)
            .withPartSize(partSize);

        // Upload part and add response to our list.
        partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());
        filePosition += partSize;
      }

      // Step 3: Complete.
      CompleteMultipartUploadRequest compRequest = 
        new CompleteMultipartUploadRequest(bucketName, keyName, initResponse.getUploadId(), partETags);
      s3Client.completeMultipartUpload(compRequest);
    } catch (Exception e) {
      s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, keyName, initResponse.getUploadId()));
      e.printStackTrace();
    }
  }

  /**
   * Displays the contents of the specified input stream as text.
   * @param input The input stream to display as text.
   * @throws IOException
   */
  private void displayTextInputStream(InputStream input) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    while (true) {
      String line = reader.readLine();
      if (line == null) break;
      System.out.println("    " + line);
    }
    System.out.println();
  }
}
