package com.neverwinter.scribengin.impl;

import org.apache.commons.chain.Context;

import com.neverwinter.scribengin.api.Filter;

public class HDFSFilter implements Filter<byte[]> {

	@Override
	public boolean execute(Context context) throws Exception {
		return !doFilter((byte[]) context.get("HDFSData"));
	}

	@Override
	public boolean doFilter(byte[] t) {
		return true;
	}

	@Override
	public boolean postprocess(Context context, Exception exception) {
		// TODO tell zk that we have actioned the message.
		return false;
	}
}
