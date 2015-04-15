package com.spikeify.converters;

import com.spikeify.Converter;

public class LongConverter implements Converter<Long, Long> {

	public boolean canConvert(Class type) {
		return Long.class.isAssignableFrom(type) || long.class.isAssignableFrom(type);	}

	@Override
	public Long fromProperty(Long property) {
		return property;
	}

	@Override
	public Long fromField(Long fieldValue) {
		return fieldValue;
	}


}
