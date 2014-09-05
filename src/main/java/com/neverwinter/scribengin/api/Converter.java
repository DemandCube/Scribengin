package com.neverwinter.scribengin.api;

import org.apache.commons.chain.Command;


//convert e.g from kafka to HDFS
//Should be called Decoder?
public interface Converter<F, T>  extends Command {

	T convert(F input);
}
