package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

public class IntegerConverter implements Converter<Integer, Long>, ConverterFactory {

	@Override
	public Converter init(Class type) {
		return this;
	}

	public boolean canConvert(Class type) {
		return Integer.class.isAssignableFrom(type) || int.class.isAssignableFrom(type);
	}

	public Integer fromProperty(Long property) {
		return property.intValue();
	}

	public Long fromField(Integer fieldValue) {
		return Long.valueOf(fieldValue);
	}

}
