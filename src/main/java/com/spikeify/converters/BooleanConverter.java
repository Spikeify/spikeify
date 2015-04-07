package com.spikeify.converters;

import com.spikeify.Converter;

public class BooleanConverter implements Converter<Boolean, Long> {

	public boolean canConvert(Class type) {
		return Boolean.class.isAssignableFrom(type) || boolean.class.isAssignableFrom(type);
	}

	public Boolean fromProperty(Long property) {
		return property != 0;
	}

	public Long fromField(Boolean fieldValue) {
		return (long) (fieldValue ? 1 : 0);
	}

}
