package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Type;

public class ShortConverter implements Converter<Short, Long>, ConverterFactory {

	@Override
	public Converter init(Type type) {
		return this;
	}

	public boolean canConvert(Class type) {
		return Short.class.isAssignableFrom(type) || short.class.isAssignableFrom(type);
	}

	public Short fromProperty(Long property) {
		return property.shortValue();
	}

	public Long fromField(Short fieldValue) {
		return Long.valueOf(fieldValue);
	}

}
