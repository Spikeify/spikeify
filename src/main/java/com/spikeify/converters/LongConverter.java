package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Field;

public class LongConverter implements Converter<Long, Long>, ConverterFactory {

	@Override
	public Converter init(Field field) {
		return this;
	}

	public boolean canConvert(Class type) {
		return Long.class.isAssignableFrom(type) || long.class.isAssignableFrom(type);
	}

	@Override
	public Long fromProperty(Long property) {
		return property;
	}

	@Override
	public Long fromField(Long fieldValue) {
		return fieldValue;
	}


}
