package com.spikeify.converters;

import com.spikeify.Converter;

public class ByteArrayConverter implements Converter<byte[], byte[]> {

	public boolean canConvert(Class type) {
		return byte[].class.isAssignableFrom(type);
	}

	public byte[] fromProperty(byte[] bytes) {
		return bytes;
	}

	public byte[] fromField(byte[] fieldValue) {
		return fieldValue;
	}

}
