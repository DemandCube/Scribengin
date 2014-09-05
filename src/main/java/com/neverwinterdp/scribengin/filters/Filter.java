package com.neverwinterdp.scribengin.filters;


/**
 * Returns true if message should proceed, false otherwise.
 * 
 * A filter can be applied anywhere along the chain. i.e after a Reader, before
 * a Converter, after a Converter, before a writer. 
 * 
 * Returning false stops that messages processing chain.
 */
public interface Filter<T> extends org.apache.commons.chain.Filter {
	boolean doFilter(T t);
}