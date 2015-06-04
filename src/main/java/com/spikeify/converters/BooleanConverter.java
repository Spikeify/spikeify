package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Field;

public class BooleanConverter implements Converter<Boolean, Long>, ConverterFactory {

	@Override
	public Converter init(Field field) {
		return this;
	}

	public boolean canConvert(Class type) {
		return Boolean.class.isAssignableFrom(type) || boolean.class.isAssignableFrom(type);
	}

	public Boolean fromProperty(Long property) {
		return property != null && property != 0;
	}

	public Long fromField(Boolean fieldValue) {
		return (long) (fieldValue ? 1 : 0);
	}

}
