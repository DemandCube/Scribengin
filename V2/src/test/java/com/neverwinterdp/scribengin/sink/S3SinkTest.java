package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.Md5Utils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.stream.sink.S3SinkStream;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class S3SinkTest.
 */

public class S3SinkTest {
	static {
		System.setProperty("log4j.configuration",
				"file:src/test/resources/log4j.properties");
	}
	/** The s3. */
	private static AmazonS3 s3;

	/** The s3 sink config. */
	private static S3SinkConfig s3SinkConfig;

	/** The sink. */
	private static S3SinkStream sink;

	/**
	 * Initialize the s3 module.
	 * 
	 * @param propFilePath
	 *            the prop file path
	 */
	public void init(String propFilePath) {
		Injector injector = Guice.createInjector(new S3Module(propFilePath,
				"topicTest", 1, false));
		sink = injector.getInstance(S3SinkStream.class);
		s3 = injector.getInstance(AmazonS3.class);
		s3SinkConfig = injector.getInstance(S3SinkConfig.class);
	}

	@Before
	public void setup() {
		// TODO clear bucket
	}

	// TODO make it work
	@Test(expected = AmazonClientException.class)
	public void testBadCredentials() throws IOException {
		System.out.println(System.getProperty("user.home"));
		System.setProperty("AWS_CREDENTIAL_PROFILES_FILE", "test");

		System.out.println(System.getProperty("AWS_CREDENTIAL_PROFILES_FILE"));
		init("s3.tuplesCountLimited.properties");
		int tuples = 8;
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		sink.commit();
	}

	@Test(expected = FileNotFoundException.class)
	public void testUploadNonExistentFile() throws IOException {
		init("s3.tuplesCountLimited.properties");
		int tuples = 8;
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		// delete files from
		for (File file : sink.getBuffer().getFiles()) {
			System.out.println("file " + file);
			System.out.println("deleted " + file.delete());
		}
		sink.commit();
	}

	@Test(expected = AmazonClientException.class)
	public void testUploadToNonExistentBucket() throws IOException {
		init("s3.noneexistentbucket.properties");
		int tuples = 8;
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		sink.commit();

	}

	@Test(expected = AmazonClientException.class)
	public void testUploadToNonWritableBucket() throws IOException {
		init("s3.tuplesCountLimited.properties");
		int tuples = 8;
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		sink.commit();
	}

	// TODO change this
	@Test(expected = AmazonClientException.class)
	public void testCreateExistingBucket() {
		init("s3.tuplesCountLimited.properties");

		boolean sinkExists = sink.prepareCommit();
		assertTrue(sinkExists);
	}

	@Test
	public void testUploadSmallFile() throws IOException {
		init("s3.tuplesCountLimited.properties");
		int tuples = s3SinkConfig.getChunkSize();// ensure its 1 file
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		List<String> fileHashes = new ArrayList<>();
		for (File file : sink.getBuffer().getFiles()) {
			fileHashes.add(new String(Md5Utils.md5AsBase64(file)));
		}
		sink.commit();
		Collection<String> commitHashes = sink.getUploadedFilePaths().values();

		assertTrue(fileHashes.containsAll(commitHashes));
		assertTrue(commitHashes.containsAll(fileHashes));

	}

	// upload 10 small to a bucket
	@Test
	public void testUploadManyFiles() throws IOException {
		init("s3.tuplesCountLimited.properties");
		int filesCount = 10;
		int tuples = s3SinkConfig.getChunkSize() * filesCount;
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		List<String> md5s = new ArrayList<>();
		for (File file : sink.getBuffer().getFiles()) {
			md5s.add(new String(Md5Utils.md5AsBase64(file)));
		}
		sink.commit();
		Collection<String> committed = sink.getUploadedFilePaths().values();

		assertTrue(md5s.containsAll(committed));
		assertTrue(committed.containsAll(md5s));
		assertEquals(filesCount, committed.size());
	}

	@Test
	public void testUploadManyFilesToManyBuckets() throws IOException {
		// is there a better way of doing this?
		init("s3.tuplesCountLimited.properties");
		String bucket2 = "nellouze";
		int filesCount = 10;
		int tuples = s3SinkConfig.getChunkSize() * filesCount;
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		List<String> md5s = new ArrayList<>();
		for (File file : sink.getBuffer().getFiles()) {
			md5s.add(new String(Md5Utils.md5AsBase64(file)));
		}
		sink.prepareCommit();
		sink.commit();
		Collection<String> committed = sink.getUploadedFilePaths().values();

		assertTrue(md5s.containsAll(committed));
		assertTrue(committed.containsAll(md5s));
		assertEquals(filesCount, committed.size());

		sink.setBucketName(bucket2);

		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		md5s = new ArrayList<>();
		for (File file : sink.getBuffer().getFiles()) {
			md5s.add(new String(Md5Utils.md5AsBase64(file)));
		}
		sink.prepareCommit();
		sink.commit();
		committed = sink.getUploadedFilePaths().values();

		assertTrue(md5s.containsAll(committed));
		assertTrue(committed.containsAll(md5s));
		assertEquals(filesCount, committed.size());

		// several inits?
		// assert object.getcontent.md5 = file.getcontent.md5
	}

	// TODO convert to test versioning config
	@Test(expected = AmazonClientException.class)
	public void testUploadFileTwice() throws IOException {
		init("s3.tuplesCountLimited.properties");
		int tuples = s3SinkConfig.getChunkSize();// ensure its 1 file
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		sink.commit();
		sink.completeCommit();

		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}

		sink.commit();
		System.out.println("NOOOOOOOOOOOOOOOOOOOOO");

	}

	@Test
	public void InterruptCommitProcess() throws IOException {
		init("s3.tuplesCountLimited.properties");
		int tuples = s3SinkConfig.getChunkSize();// ensure its 1 file
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		Thread thread = new Thread() {

			@Override
			public void run() {
				try {
					sink.commit();
				} catch (Exception e) {
					System.out.println("Interupted commit");
				}
			}
		};
		thread.start();
		try {
			thread.interrupt();
		} catch (Exception e) {
			System.out.println("Exception handled " + e);
		}

		sink.rollBack();
		Assert.assertTrue(sink
				.getAmazonS3()
				.getObjectMetadata(sink.getBucketName(),
						sink.getUploadedFilePaths().keySet().iterator().next())
				.getContentLength() == 0);
		// assert s3 doesn't contain file
	}

	@Test(expected = AmazonClientException.class)
	public void testUploadTenBigFiles() {

	}

	@Test(expected = AmazonClientException.class)
	public void testUploadTenBigFilesToTenBuckets() {

	}

	// upload 10000 small files using 100 as offsetPerPartition and 10 as chunk
	// size
	@Test
	public void testObeysInstallProperties() {

	}

	@Test
	// upload 100 files using 10 as chunk size
	public void testCountUploadedFiles() {

	}

	// Upload 10 5GB files to a bucket
	@Test
	public void testUploadBigFiles() {

	}

	// Upload 10 5GB files to 10 buckets (100 files total)
	@Test
	public void testUploadTenBuckets() {

	}

	/**
	 * Tuples count limited.
	 * 
	 * @throws IOException
	 *             the IO exception
	 * @throws InterruptedException
	 */
	@Test
	public void tuplesCountLimited() throws IOException, InterruptedException {

		init("s3.tuplesCountLimited.properties");
		int i = 0;
		for (i = 0; i < 8; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		assertTrue(sink.prepareCommit());
		assertTrue(sink.commit());
		assertTrue(sink.completeCommit());
		checkFilesExist();

	}

	/**
	 * Tuples time limited.
	 * 
	 * @throws IOException
	 *             the IO exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	@Test
	public void tuplesTimeLimited() throws IOException, InterruptedException {

		init("s3.tuplesTimeLimited.properties");
		int i = 0;
		for (i = 0; i < 8; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i),
					new byte[1024], new CommitLogEntry("key", i, i))));
			Thread.sleep(1000);
		}
		assertTrue(sink.prepareCommit());
		assertTrue(sink.commit());
		assertTrue(sink.completeCommit());
		checkFilesExist();
	}

	/**
	 * Tuples size limited.
	 * 
	 * @throws IOException
	 *             the IO exception
	 * @throws InterruptedException
	 */
	@Test
	public void tuplesSizeLimited() throws IOException, InterruptedException {

		init("s3.tuplesSizeLimited.properties");
		int i = 0;
		for (i = 0; i < 8; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i),
					new byte[1024], new CommitLogEntry("key", i, i))));
		}
		assertTrue(sink.prepareCommit());
		assertTrue(sink.commit());
		assertTrue(sink.completeCommit());
		checkFilesExist();
	}

	/**
	 * Test rollback.
	 * 
	 * @throws IOException
	 *             the IO exception
	 */
	@Test
	public void testRollback() throws IOException {

		init("s3.tuplesCountLimited.properties");
		int i = 0;
		for (i = 0; i < 8; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		assertTrue(sink.prepareCommit());
		assertTrue(sink.commit());
		assertTrue(sink.rollBack());

	}

	/**
	 * Check files exist.
	 * 
	 * @throws InterruptedException
	 */

	public void checkFilesExist() {
		ObjectListing list = s3.listObjects(s3SinkConfig.getBucketName(),
				"topicTest/1/offset=0");
		assertTrue(list.getObjectSummaries().size() == 4);
		String path;
		for (int j = 0; j < 8; j += 2) {
			path = +j + "_" + (j + 1);
			S3Object s3Object1 = s3.getObject(s3SinkConfig.getBucketName()
					+ "/topicTest/1/offset=0", path);
			assertTrue(s3Object1 != null);

		}
	}
}
