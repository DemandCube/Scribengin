package com.neverwinterdp.scribengin.utils;

public class Partition {
	String host;
	int jmx_port;
	int port;
	String timestamp;
	int version;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getJmx_port() {
		return jmx_port;
	}

	public void setJmx_port(int jmx_port) {
		this.jmx_port = jmx_port;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public String toString() {
		return "Partition [host=" + host + ", jmx_port=" + jmx_port + ", port="
				+ port + ", timestamp=" + timestamp + ", version=" + version
				+ "]";
	}
}