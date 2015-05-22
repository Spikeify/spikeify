package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Field;

public class ByteConverter implements Converter<Byte, Long>, ConverterFactory {

	@Override
	public Converter init(Field field) {
		return this;
	}

	public boolean canConvert(Class type) {
		return Byte.class.isAssignableFrom(type) || byte.class.isAssignableFrom(type);
	}

	public Byte fromProperty(Long property) {
		return property.byteValue();
	}

	public Long fromField(Byte fieldValue) {
		return Long.valueOf(fieldValue);
	}

}
