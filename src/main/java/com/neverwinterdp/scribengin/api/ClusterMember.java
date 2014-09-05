package com.neverwinterdp.scribengin.api;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.Service;

//TODO jmx metrics

/*
 * Possible metrics
 * 
 * uptime
 * 
 * */

/**
 * How to work Register self as cluster member 2. Initializer reader, tell it
 * what to read where to read from, could be kafka, kinesis, sparkngin 3. Send
 * it to a funnel that will split into multiple streams 4. Send it to a
 * converter that will convert from source format to sink format 5. Writer
 * writes to end. Could be HDFS, HBASE, HIVE, e.t.c
 * 
 */

public interface ClusterMember extends Service, Runnable {
	final MetricRegistry metrics = new MetricRegistry();
}
