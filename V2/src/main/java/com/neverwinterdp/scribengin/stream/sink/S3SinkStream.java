package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.neverwinterdp.scribengin.buffer.SinkBuffer;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class S3SinkStream.
 * 
 * The default config is to allow versioning. But for space constraints we
 * delete older versions in the postcommit method. This setting can be easily
 * changed via the .....
 */

public class S3SinkStream implements SinkStream {

	/** The name. */
	private String name;

	/** The s3. */
	private AmazonS3 s3Client;

	/** The bucket name. */
	private String bucketName;

	/** The partitioner. */
	private SinkPartitioner partitioner;

	/** The memory buffer. */
	private SinkBuffer buffer;

	/** The local tmp dir. */
	private String localTmpDir;

	/** The logger. */
	private static Logger logger;

	/** The region name. */
	private Regions regionName;

	/** The valid s3 sink. */
	private boolean validS3Sink = false;

	/** A map holding file-path name and it corresponding MD5 hash */
	private Map<String, String> uploadedFilesPath = new HashMap<>();

	private String bucketVersionConfig;

	/**
	 * The Constructor.
	 * 
	 * @param s3Client
	 *            the s3 client
	 * @param partitioner
	 *            the partitioner
	 * @param config
	 *            the configuration
	 */
	@Inject
	public S3SinkStream(AmazonS3 s3Client, SinkPartitioner partitioner,
			S3SinkConfig config) {
		logger = LogManager.getLogger(S3SinkStream.class);
		this.partitioner = partitioner;
		this.bucketName = config.getBucketName();
		this.localTmpDir = config.getLocalTmpDir();
		this.bucketVersionConfig = config.getBucketVersioningConfig();
		this.buffer = new SinkBuffer(this.partitioner, config);
		this.s3Client = s3Client;
		this.regionName = Regions.fromName(config.getRegionName());
		Region region = Region.getRegion(regionName);
		this.s3Client.setRegion(region);
	}

	/**
	 * Checks if it is time to commit.
	 * 
	 * @return true, if checks if is time to commit
	 */
	public boolean isSaturated() {
		return buffer.isSaturated();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.neverwinterdp.scribengin.stream.Stream#prepareCommit()
	 */
	// This is where we create the bucket.
	@Override
	public boolean prepareCommit() {
		logger.info("prepareCommit");
		if (!validS3Sink) {

			try {
				// check if bucket exist

				if (!s3Client.doesBucketExist(bucketName)) {
					System.out.println("bucket does not exist.");
					logger.info("Bucket does not Exist");
					s3Client.createBucket(bucketName);

				}

				logger.info("Bucket Exist");
				/*
				 * BucketVersioningConfiguration configuration = new
				 * BucketVersioningConfiguration( bucketVersionConfig);
				 * SetBucketVersioningConfigurationRequest request = new
				 * SetBucketVersioningConfigurationRequest( bucketName,
				 * configuration);
				 * s3Client.setBucketVersioningConfiguration(request);
				 */
				AccessControlList acl = s3Client.getBucketAcl(bucketName);
				List<Permission> permissions = new ArrayList<Permission>();
				for (Grant grant : acl.getGrants()) {
					permissions.add(grant.getPermission());
				}
				if (permissions.contains(Permission.FullControl)
						|| permissions.contains(Permission.Write)) {
					validS3Sink = true;
				}

			} catch (Exception e) {
				logger.error("Caught Exception when perparing commit "
						+ e.getMessage());
				validS3Sink = false;
			}
		} else {
			validS3Sink = true;
		}
		logger.info("validS3Sink = " + validS3Sink);
		return validS3Sink;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.neverwinterdp.scribengin.stream.Stream#commit()
	 */
	@Override
	public boolean commit() throws IOException {
		System.out.println("committing");
		logger.info("commit");
		buffer.purgeMemoryToDisk();
		String path;
		File file;
		while (buffer.getFilesCount() > 0) {
			// the file path on local is similar to its path on s3, just change
			// tmp folder by bucket name
			file = buffer.pollFromDisk();
			System.out.println("getFilesCount " + buffer.getFilesCount() + " ");

			path = file.getPath();
			logger.info("file path " + path);
			String key2 = path.substring(path.indexOf(localTmpDir) + 5,
					path.length());
			System.out.println("path exists " + path);
			System.out.println("key2 " + key2);

			logger.info("Uploading a new object to S3 from a file\n");
			try {
				// upload to S3
				PutObjectResult result = s3Client.putObject(bucketName, key2,
						file);

				System.out.println("result " + result.getContentMd5());
				uploadedFilesPath.put(key2, result.getContentMd5());
			} catch (AmazonServiceException ase) {
				logger.error("Caught an AmazonServiceException. "
						+ ase.getMessage());
				if (ase.getCause() instanceof IOException) {
					throw (IOException) ase.getCause();
				}
				System.out.println(ase);
				Throwables.propagate(ase);

			} catch (AmazonClientException ace) {
				logger.error("Caught an AmazonClientException. "
						+ ace.getMessage());
				if (ace.getCause() instanceof IOException) {
					throw (IOException) ace.getCause();
				}
				System.out.println(ace);
				Throwables.propagate(ace);
			}
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.neverwinterdp.scribengin.stream.Stream#clearBuffer()
	 */
	@Override
	public boolean clearBuffer() {
		logger.info("clear buffer");
		try {
			buffer.clear();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.neverwinterdp.scribengin.stream.Stream#completeCommit()
	 */
	@Override
	// TODO check that files have correctly been uploaded to s3?
	public boolean completeCommit() {
		logger.info("completeCommit");
		try {
			buffer.clear();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		uploadedFilesPath.clear();
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#bufferTuple(com.
	 * neverwinterdp.scribengin.tuple.Tuple)
	 */
	@Override
	public boolean bufferTuple(Tuple tuple) {
		logger.info("bufferTuple");
		try {
			// TODO buffer.add doesn't throw exception
			buffer.add(tuple);
			return true;
		} catch (Exception e) {
			logger.error("Caught Exception when  buffering Tuple"
					+ e.getMessage());
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#rollBack()
	 */
	// TODO if we are using versioning delete the version
	@Override
	public boolean rollBack() {
		boolean retVal = true;
		// TODO remove from uploadedFilesPath
		for (String path : uploadedFilesPath.keySet()) {
			try {
				System.out.println("deleted " + path);
				s3Client.deleteObject(bucketName, path);
			} catch (Exception e) {
				logger.error("Caught Exception when rolling back "
						+ e.getMessage());
				retVal = false;
			}
		}
		uploadedFilesPath.clear();
		return retVal;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.neverwinterdp.scribengin.stream.sink.SinkStream#setSinkPartitioner(
	 * com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner)
	 */
	@Override
	public void setSinkPartitioner(SinkPartitioner sinkPartitioner) {
		this.partitioner = sinkPartitioner;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#getBufferSize()
	 */
	@Override
	// TODO improve name.
	// Does it return tuples in buffer, size of tuples in buffer or max holdable
	// tuples in buffer
	public long getBufferSize() {
		return buffer.size();
	}

	public SinkBuffer getBuffer() {
		return buffer;
	}

	public AmazonS3 getAmazonS3() {
		return s3Client;
	}

	public Map<String, String> getUploadedFilePaths() {
		return uploadedFilesPath;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.neverwinterdp.scribengin.stream.sink.SinkStream#getName()
	 */
	@Override
	public String getName() {
		return name;
	}
}
