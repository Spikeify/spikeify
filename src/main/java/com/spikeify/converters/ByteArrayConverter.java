package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Field;

public class ByteArrayConverter implements Converter<byte[], byte[]>, ConverterFactory {

	@Override
	public Converter init(Field field) {
		return this;
	}

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
