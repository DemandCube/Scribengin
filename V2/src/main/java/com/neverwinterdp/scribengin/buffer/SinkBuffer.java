package com.neverwinterdp.scribengin.buffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neverwinterdp.scribengin.stream.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class SinkBuffer.
 */
public final class SinkBuffer {

	/** The max tuples. */
	private long maxTuplesInMemory;

	/** The max buffer size. */
	private long maxTuplesSizeInMemory;

	/** The max buffering time. */
	private long maxBufferingTimeInMemory;

	/** The max tuples. */
	private long maxTuplesOnDisk;

	/** The max buffer size. */
	private long maxBufferSizeOnDisk;

	/** The max buffering time. */
	private long maxBufferingTimeOnDisk;

	/** The start time. */
	// TODO reset this after purge to memory
	private long startBufferingTimeInMemory;

	/** The start time. */
	private long startBufferingTimeOnDisk;



	/** The files. */
	private LinkedList<File> files = new LinkedList<File>();

	/** The chunk size. */
	private int chunkSize;

	/** The partitioner. */
	private SinkPartitioner partitioner;

	/** The memory buffering enabled. */
	private boolean memoryBufferingEnabled;

	/** The logger. */
	private static Logger logger;
	/** The buffer. */
	private LinkedList<Tuple> tuples = new LinkedList<Tuple>();

	/** The local tmp dir. */
	private String localTmpDir;

	private Thread bufferThread;

	private boolean active = true;

	/**
	 * The Constructor.
	 * 
	 * @param partitioner
	 *            the partitioner
	 * @param config
	 *            the configuration
	 */
	public SinkBuffer(SinkPartitioner partitioner, S3SinkConfig config) {
		this.localTmpDir = System.getProperty("java.io.tmpdir");
		this.maxTuplesSizeInMemory = config.getMemoryMaxBufferSize();
		this.maxBufferingTimeInMemory = config.getMemoryMaxBufferingTime();
		this.maxTuplesOnDisk = config.getMemoryMaxTuples();
		this.maxBufferSizeOnDisk = config.getDiskMaxBufferSize();
		this.maxBufferingTimeOnDisk = config.getDiskMaxBufferingTime();
		this.maxTuplesInMemory = config.getMemoryMaxTuples();
		//this.mappedByteBufferSize = config.getMappedByteBufferSize();
		this.partitioner = partitioner;
		this.chunkSize = config.getChunkSize();
		memoryBufferingEnabled = config.isMemoryBufferingEnabled();
		bufferThread = new Thread() {
			public void run() {
				try {
					runProcessLoop();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		bufferThread.start();
		logger = LogManager.getLogger(SinkBuffer.class);
	}

	private void setProcessLoopActive(boolean active) {
		this.active = active;
	}

	/**
	 * Adds the Tuple to the buffer.
	 * 
	 * @param tuple
	 *            the tuple
	 */
	// TODO discuss max tuples to buffer. 1000000 tuples of byte[10] fill 1.6GB
	public boolean add(Tuple tuple) {
		if (memoryBufferingEnabled) {
			// TODO nizar what is happening here?
			if (!checkMemoryAvailability(tuple.getData().length)) {
				setProcessLoopActive(true);
				// tuplesCountInMemory = 0;
				// tuplesSizeInMemory = 0;
			}
			tuples.add(tuple);
		} else {
			addToDisk(tuple);
			updateDiskState();
		}
		return true;
	}

	/**
	 * Adds the to disk.
	 * 
	 * @param tuple
	 *            the tuple
	 * @return true, if adds the to disk
	 */
	private LinkedList<Tuple> tuplesChunk = new LinkedList<Tuple>();

	private boolean addToDisk(Tuple tuple) {
		boolean success = false;

		RandomAccessFile randomAccessFile = null;
		FileChannel fileChannel = null;
		try {
			tuplesChunk.add(tuple);
			// write every chunk of tuples in one file
			if (tuplesChunk.size() == chunkSize) {
				try {
					long startOffset = tuplesChunk.getFirst()
							.getCommitLogEntry().getStartOffset();
					long endOffset = tuplesChunk.getLast().getCommitLogEntry()
							.getEndOffset();
					// call partitioner to get the path of the file depending on
					// the offset
					// the path will be later used to deduce the s3 path
					String path = localTmpDir + "/"
							+ partitioner.getPartition(startOffset, endOffset);
					// create file using the path
					File file = new File(path);
					File parent = file.getParentFile();
					if (!parent.exists() && !parent.mkdirs()) {
						throw new IllegalStateException("Couldn't create dir: "
								+ parent);
					}
					// write a memory mapped file
					int start = 0;
					randomAccessFile = new RandomAccessFile(file, "rw");
					fileChannel = randomAccessFile.getChannel();
					MappedByteBuffer mem;
					for (Tuple t : tuplesChunk) {

						mem = fileChannel.map(FileChannel.MapMode.READ_WRITE,
								start, t.getData().length + 1);
						start += t.getData().length + 1;
						mem.put(t.getData());
						mem.put("\n".getBytes());
					}
					// add the file to the list of file created
					success = files.add(file);
					tuplesChunk.clear();
				} catch (Exception e) {
					e.printStackTrace();
				}

				finally {

					randomAccessFile.close();
					fileChannel.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return success;
	}

	private void runProcessLoop() throws InterruptedException {
		while (true) {
			if (active) {
				purgeMemoryToDisk();
			}
			Thread.sleep(1000);
		}
	}

	/**
	 * Writes all tuples to disk creating more in-memory space. A few tuples
	 * that couldn't complete a chunk will be left in-memory.
	 */
	public void purgeMemoryToDisk() {
		logger.info("purge Memory To Disk");
		int toRemove = tuples.size() - (tuples.size() % chunkSize);
		while (toRemove != 0) {
			addToDisk(tuples.poll());
			toRemove--;
		}
		updateDiskState();
		setProcessLoopActive(false);
	}

	/**
	 * Check memory availability.
	 * 
	 * @param newTupleSize
	 *            the new tuple size
	 * @return true, if check memory availability
	 */
	// TODO check space availability on disk as well?
	private boolean checkMemoryAvailability(int newTupleSize) {

		if (startBufferingTimeInMemory == 0) {
			startBufferingTimeInMemory = System.currentTimeMillis();
		}
		if (tuples.size() == maxTuplesInMemory
				|| getTuplesSizeInMemory() + newTupleSize > maxTuplesSizeInMemory
				|| (System.currentTimeMillis() - startBufferingTimeInMemory) > maxBufferingTimeInMemory) {
			return false;
		}
		return true;
	}

	private int getTuplesSizeInMemory() {
		int size = 0;
		for (Tuple tuple : tuples) {
			size += tuple.getData().length;
		}
		return size;
	}

	/**
	 * Update disk state.
	 */
	private void updateDiskState() {

		if (startBufferingTimeOnDisk == 0) {
			startBufferingTimeOnDisk = System.currentTimeMillis();
		}
		/*
		 * if (files.size() > maxTuplesOnDisk || getTuplesSizeOnDisk() >
		 * maxBufferSizeOnDisk || (System.currentTimeMillis() -
		 * startBufferingTimeOnDisk) > maxBufferingTimeOnDisk) { saturated =
		 * true; }
		 */
	}

	private long getTuplesSizeOnDisk() {
		long size = 0;
		for (File file : files) {
			size = file.length();
		}
		return size;
	}

	/**
	 * Clear tuples in memory and on disk.
	 * 
	 * @throws IOException
	 */
	public void clear() throws IOException {
		String separator = System.getProperty("file.separator");
		File file = new File(localTmpDir + separator
				+ partitioner.getPartition());
		try {
			FileUtils.deleteDirectory(file);

		} catch (Exception e) {
			e.printStackTrace();
		}
		tuples.clear();
		files.clear();
		startBufferingTimeInMemory = 0;
		startBufferingTimeOnDisk = 0;
	}

	/**
	 * Gets the files size.
	 * 
	 * @return the files size
	 */
	public int getFilesCount() {
		return files.size();
	}

	/**
	 * Poll from disk.
	 * 
	 * @return the file
	 */
	// TODO name suggests that we actually read from disk?
	public File pollFromDisk() {
		return files.poll();
	}

	/**
	 * Checks if is saturated.
	 * 
	 * @return true, if checks if is saturated
	 */
	public boolean isSaturated() {

		return getIsSaturated();
	}

	/**
	 * @return
	 */
	private boolean getIsSaturated() {
		return files.size() > maxTuplesOnDisk
				|| getTuplesSizeOnDisk() > maxBufferSizeOnDisk
				|| (startBufferingTimeOnDisk != 0 && (System
						.currentTimeMillis() - startBufferingTimeOnDisk) > maxBufferingTimeOnDisk);
	}

	/*
	 * Note that there are other methods for reading on-File size vs in-Memory
	 * sizes
	 */
	public int size() {
		return tuples.size() + (files.size() * chunkSize);
	}

	public int tuplesInMemory() {
		return tuples.size();
	}

	public int tuplesOnDisk() {
		return (files.size() * chunkSize);
	}

	public boolean isEmpty() {
		return tuples.isEmpty() && files.isEmpty();
	}

	public boolean contains(Object o) {
		return tuples.contains(o);
	}

	// TODO also get file having tuple and remove
	public boolean remove(Object o) {
		return tuples.remove(o) && files.remove(o);
	}

	public boolean containsAll(Collection<?> collection) {

		return tuples.containsAll(collection);
	}

	public boolean addAll(Collection<? extends Tuple> ccollection) {
		boolean success = false;
		for (Tuple tuple : ccollection) {
			// TODO confirm if it does what it should
			success &= add(tuple);
		}
		return success;
	}

	// TODO and remove from files as well
	public boolean removeAll(Collection<?> collection) {
		return tuples.removeAll(collection);
	}

	/*
	 * This methods exists solely for testing purposes.
	 */
	public LinkedList<File> getFiles() {
		return files;

	}
}
