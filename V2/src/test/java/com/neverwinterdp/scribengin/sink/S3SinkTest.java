package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.Md5Utils;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
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

	private String bucketName;

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
		bucketName = s3SinkConfig.getBucketName();
	}

	@After
	public void afterTest() {
		System.out.println("buckets "
				+ System.getenv("AWS_CREDENTIAL_PROFILES_FILE"));
		init("s3.tuplesCountLimited.properties");
		for (S3ObjectSummary object : s3.listObjects(bucketName)
				.getObjectSummaries()) {
			System.out.println("deleting " + object.getKey());
			s3.deleteObject(bucketName, object.getKey());
		}

	}

	@Test(expected = AmazonS3Exception.class)
	public void testBadCredentials() throws IOException {
		Map<String, String> env = System.getenv();
		Map<String, String> fakeEnv = new ConcurrentHashMap<String, String>();
		try {
			File temp = File.createTempFile("fakeCredentials", ".tmp");
			String credentials = " #User Name=artfullyContrived \r\n"
					+ "[default] \r\n"
					+ "aws_access_key_id=FAKESTACCESSKEYEVER7338 \r\n"
					+ "aws_secret_access_key=ANOTHEREXTREMELYFAKESECRETACCESSKEY1084 \r\n";

			Files.append(credentials, temp, Charsets.UTF_8);
			fakeEnv.put("AWS_CREDENTIAL_PROFILES_FILE", temp.getCanonicalPath());
			fakeEnv.putAll(env);
			setEnv(fakeEnv);

			System.out.println("credentitals file "
					+ System.getenv("AWS_CREDENTIAL_PROFILES_FILE"));
			init("s3.tuplesCountLimited.properties");
			int tuples = s3SinkConfig.getChunkSize();
			for (int i = 0; i < tuples; i++) {
				assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i),
						Integer.toString(i).getBytes(), new CommitLogEntry(
								"key", i, i))));
			}
			sink.commit();
		} finally {
			fakeEnv.remove("AWS_CREDENTIAL_PROFILES_FILE");
			setEnv(fakeEnv);
			System.out.println("buckets "
					+ System.getenv("AWS_CREDENTIAL_PROFILES_FILE"));
		}
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
		for (String file : sink.getBuffer().getFiles()) {
			System.out.println("file " + file);
			System.out.println("deleted " + new File(file).delete());
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
		init("s3.noneexistentbucket.properties");
		int tuples = 8;
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		sink.commit();
	}

	// TODO Our sink checks for existence of bucket before attempting to create
	// a new one
	@Test
	public void testCreateExistingBucket() {
		init("s3.tuplesCountLimited.properties");

		boolean sinkExists = sink.prepareCommit();
		s3.doesBucketExist(s3SinkConfig.getBucketName());
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

		for (String fileName : sink.getBuffer().getFiles()) {
			fileHashes
					.add(new String(Md5Utils.md5AsBase64(new File(fileName))));
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
		for (String file : sink.getBuffer().getFiles()) {
			md5s.add(new String(Md5Utils.md5AsBase64(new File(file))));
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
		for (String file : sink.getBuffer().getFiles()) {
			md5s.add(new String(Md5Utils.md5AsBase64(new File(file))));
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
		for (String file : sink.getBuffer().getFiles()) {
			md5s.add(new String(Md5Utils.md5AsBase64(new File(file))));
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
	public void testSimpleRollBack() throws IOException {
		init("s3.tuplesCountLimited.properties");
		int tuples = s3SinkConfig.getChunkSize() * 2;
		for (int i = 0; i < tuples; i++) {
			assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer
					.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
		}
		sink.commit();
		sink.rollBack();

		System.out.println("we have " + sink.getUploadedFilePaths().keySet());
		System.out.println(s3.listObjects(bucketName).getObjectSummaries()
				.size());
		assertTrue(s3.listObjects(bucketName).getObjectSummaries().size() == 0);
	}

	@Test
	public void testInteruptCommit() throws IOException {
		// commit in thread, wait a minute, interupt, logback, files in bucket
		// should be ==0;
		init("s3.tuplesCountLimited.properties");
		int tuples = s3SinkConfig.getChunkSize() * 10;
		for (int i = 0; i < tuples; i++) {
			sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i)
					.getBytes(), new CommitLogEntry("key", i, i)));
		}

		Runnable runnable = new Runnable() {

			@Override
			public void run() {
				try {
					sink.commit();
				} catch (IOException e) {
					System.out.println("we were interupted? " + e);
				}
			}
		};

		Thread thread = new Thread(runnable);
		thread.start();

		try {
			Thread.sleep(2000);
			thread.interrupt();
		} catch (Exception e) {
			e.printStackTrace();
		}
		sink.rollBack();

		System.out.println("we have " + sink.getUploadedFilePaths().keySet());
		assertTrue(s3.listObjects(bucketName).getObjectSummaries().size() == 0);
	}

	// Upload 10 5GB files to a bucket
	@Test(expected = IllegalArgumentException.class)
	@Ignore
	public void testUploadOneBigFiles() throws IOException {
		init("s3.largeFiles.properties");

		long chunks = 13629999L;
		Tuple tuple = null;

		System.out.println("file size " + readableFileSize(chunks * 394L));
		for (int i = 0; i < chunks; i++) {
			tuple = new Tuple(Integer.toString(i), Integer.toString(i)
					.getBytes(), new CommitLogEntry("key", i, i));

			assertTrue(sink.bufferTuple(tuple));
		}
		sink.commit();
		assertTrue(s3.listObjects(bucketName).getObjectSummaries().size() == 0);
	}

	// Attempt to upload 10 4GB files
	@Test(expected = AmazonClientException.class)
	@Ignore
	public void testUploadTenBigFilesToTenBuckets() throws IOException {
		init("s3.4GBFiles.properties");

		long chunks = s3SinkConfig.getChunkSize() * 4;
		Tuple tuple = null;

		System.out.println("file size " + readableFileSize(chunks * 394L));
		for (int i = 0; i < chunks; i++) {
			tuple = new Tuple(Integer.toString(i), Integer.toString(i)
					.getBytes(), new CommitLogEntry("key", i, i));

			assertTrue(sink.bufferTuple(tuple));
		}
		sink.commit();
		assertTrue(s3.listObjects(bucketName).getObjectSummaries().size() == 4);
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
		for (S3ObjectSummary object : s3.listObjects(bucketName)
				.getObjectSummaries()) {
			System.out.println("deleting " + object.getKey());
			// s3.deleteObject(bucketName, object.getKey());
		}

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
				"topicTest/1/20");
		System.out.println("ngapi " + list.getObjectSummaries().size());
		assertTrue(list.getObjectSummaries().size() == 4);
		
	}

	private String readableFileSize(long size) {
		if (size <= 0)
			return "0";
		final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
		int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
		return new DecimalFormat("#,##0.#").format(size
				/ Math.pow(1024, digitGroups))
				+ " " + units[digitGroups];
	}

	public int sizeof(Object obj) throws IOException {

		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(
				byteOutputStream);

		objectOutputStream.writeObject(obj);
		objectOutputStream.flush();
		objectOutputStream.close();

		return byteOutputStream.toByteArray().length;
	}

	protected static void setEnv(Map<String, String> newenv) {
		try {
			Class<?> processEnvironmentClass = Class
					.forName("java.lang.ProcessEnvironment");
			Field theEnvironmentField = processEnvironmentClass
					.getDeclaredField("theEnvironment");
			theEnvironmentField.setAccessible(true);
			Map<String, String> env = (Map<String, String>) theEnvironmentField
					.get(null);
			env.putAll(newenv);
			Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
					.getDeclaredField("theCaseInsensitiveEnvironment");
			theCaseInsensitiveEnvironmentField.setAccessible(true);
			Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField
					.get(null);
			cienv.putAll(newenv);
		} catch (NoSuchFieldException e) {
			try {
				Class[] classes = Collections.class.getDeclaredClasses();
				Map<String, String> env = System.getenv();
				for (Class cl : classes) {
					if ("java.util.Collections$UnmodifiableMap".equals(cl
							.getName())) {
						Field field = cl.getDeclaredField("m");
						field.setAccessible(true);
						Object obj = field.get(env);
						Map<String, String> map = (Map<String, String>) obj;
						map.clear();
						map.putAll(newenv);
					}
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
}
