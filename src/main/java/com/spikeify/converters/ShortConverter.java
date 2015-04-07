package com.spikeify.converters;

import com.spikeify.Converter;

public class ShortConverter implements Converter<Short, Long> {

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
