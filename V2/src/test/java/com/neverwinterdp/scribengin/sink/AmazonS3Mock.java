package com.neverwinterdp.scribengin.sink;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketPolicyRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAclRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyRequest;
import com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketPolicyRequest;
import com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;

public class AmazonS3Mock implements AmazonS3 {
  private boolean credentialsNotFoundError;
  private boolean badCredentialsError;
  private boolean bucketPermissionError;
  
  

  public void simulateCredentialsNotFoundError() {
    this.credentialsNotFoundError = true;
  }

  public void simulateBadCredentialsError() {
    this.badCredentialsError = true;
  }

  public void simulateBucketPermissionError() {
    this.bucketPermissionError = true;
  }

  @Override
  public void setEndpoint(String endpoint) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setRegion(Region region) throws IllegalArgumentException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setS3ClientOptions(S3ClientOptions clientOptions) {
    // TODO Auto-generated method stub

  }

  @Override
  public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public ObjectListing listObjects(String bucketName) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectListing listObjects(String bucketName, String prefix) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker,
      String delimiter, Integer maxResults) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VersionListing listVersions(ListVersionsRequest listVersionsRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Owner getS3AccountOwner() throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean doesBucketExist(String bucketName) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<Bucket> listBuckets() throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getBucketLocation(String bucketName) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Bucket createBucket(CreateBucketRequest createBucketRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Bucket createBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
    if(bucketPermissionError){
      throw new AmazonServiceException("");
    }
    return null;
  }

  @Override
  public Bucket createBucket(String bucketName, com.amazonaws.services.s3.model.Region region)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Bucket createBucket(String bucketName, String region) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key, String versionId) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setObjectAcl(String bucketName, String key, AccessControlList acl) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public AccessControlList getBucketAcl(String bucketName) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketAcl(SetBucketAclRequest setBucketAclRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketAcl(String bucketName, AccessControlList acl) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBucketAcl(String bucketName, CannedAccessControlList acl) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public ObjectMetadata getObjectMetadata(String bucketName, String key) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public S3Object getObject(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public S3Object getObject(GetObjectRequest getObjectRequest) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteBucket(DeleteBucketRequest deleteBucketRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public PutObjectResult putObject(PutObjectRequest putObjectRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, File file) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName,
      String destinationKey) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CopyPartResult copyPart(CopyPartRequest copyPartRequest) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteObject(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteObject(DeleteObjectRequest deleteObjectRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteVersion(String bucketName, String key, String versionId) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteVersion(DeleteVersionRequest deleteVersionRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketVersioningConfiguration(
      SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketLifecycleConfiguration(String bucketName,
      BucketLifecycleConfiguration bucketLifecycleConfiguration) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBucketLifecycleConfiguration(
      SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketLifecycleConfiguration(String bucketName) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketLifecycleConfiguration(
      DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest) {
    // TODO Auto-generated method stub

  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketCrossOriginConfiguration(String bucketName,
      BucketCrossOriginConfiguration bucketCrossOriginConfiguration) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBucketCrossOriginConfiguration(
      SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketCrossOriginConfiguration(String bucketName) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketCrossOriginConfiguration(
      DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest) {
    // TODO Auto-generated method stub

  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketTaggingConfiguration(String bucketName, BucketTaggingConfiguration bucketTaggingConfiguration) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketTaggingConfiguration(String bucketName) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketTaggingConfiguration(
      DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest) {
    // TODO Auto-generated method stub

  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketNotificationConfiguration(
      SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBucketNotificationConfiguration(String bucketName,
      BucketNotificationConfiguration bucketNotificationConfiguration) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(
      GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketWebsiteConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketWebsiteConfiguration(
      DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public BucketPolicy getBucketPolicy(String bucketName) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setBucketPolicy(String bucketName, String policyText) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketPolicy(String bucketName) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration) throws AmazonClientException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method)
      throws AmazonClientException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest) throws AmazonClientException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public UploadPartResult uploadPart(UploadPartRequest request) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PartListing listParts(ListPartsRequest request) throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void abortMultipartUpload(AbortMultipartUploadRequest request) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
      throws AmazonClientException, AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws AmazonClientException,
      AmazonServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void restoreObject(RestoreObjectRequest request) throws AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void restoreObject(String bucketName, String key, int expirationInDays) throws AmazonServiceException {
    // TODO Auto-generated method stub

  }

  @Override
  public void enableRequesterPays(String bucketName) throws AmazonServiceException, AmazonClientException {
    // TODO Auto-generated method stub

  }

  @Override
  public void disableRequesterPays(String bucketName) throws AmazonServiceException, AmazonClientException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isRequesterPaysEnabled(String bucketName) throws AmazonServiceException, AmazonClientException {
    // TODO Auto-generated method stub
    return false;
  }

}
