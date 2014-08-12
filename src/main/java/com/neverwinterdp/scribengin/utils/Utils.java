package com.neverwinterdp.scribengin.utils;

import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Charsets;

public class Utils {

	/**
	 * Deserializer from JSON string in a byte array to a map.
	 */
	private static ObjectReader reader;
	static ObjectMapper mapper = new ObjectMapper();
	static {
	}

	/**
	 * Convert a byte array containing a JSON string to a map of key/value
	 * pairs.
	 * 
	 * @param bytes
	 *            byte array containing the key/value pairs
	 * 
	 * @return a map containing the key/values
	 */
	public static Map<String, String> toMap(byte[] bytes) {

		reader = mapper.reader(Map.class);

		if (bytes == null || bytes.length == 0) {
			return Collections.emptyMap();
		}
		try {
			return reader.readValue(bytes);
		} catch (Exception e) {
			String contents;
			contents = new String(bytes, Charsets.UTF_8);
			throw new IllegalArgumentException(
					"Error parsing JSON string: " + contents, e);
		}
	}

	public static <T> T toClass(byte[] data, Class<T> clazz) {

		reader = mapper.reader(clazz);
		if (data == null || data.length == 0) {
			return null;
		}
		try {
			return reader.readValue(data);
		} catch (Exception e) {
			String contents;
			contents = new String(data, Charsets.UTF_8);
			throw new IllegalArgumentException(
					"Error parsing JSON string: " + contents, e);
		}
	}
}
