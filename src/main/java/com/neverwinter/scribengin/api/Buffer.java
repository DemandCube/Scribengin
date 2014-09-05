package com.neverwinter.scribengin.api;

import java.util.Queue;

import org.apache.commons.chain.Filter;

/*
 * for when its expensive to process a single message it might be better to buffer the messages from a reader then process them as a batch
 * */
public interface Buffer<T> extends Filter {

	Queue<T> buffer(T t);
}
