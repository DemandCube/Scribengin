package com.neverwinterdp.scribengin.s3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class MultipartUpload {

  public static void main(String[] args) throws IOException {
    String bucketName = "tuan-test";
    String keyName = "multipart-upload";
    String filePath = "build.gradle";

    //AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
    AmazonS3 s3Client = new AmazonS3Client();
    Region region = Region.getRegion(Regions.US_WEST_2);
    //Region region = Region.getRegion(Regions.SA_EAST_1);
    s3Client.setRegion(region);
    
    System.out.println("Creating bucket " + bucketName + "\n");
    //s3Client.createBucket(bucketName);
    
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
}