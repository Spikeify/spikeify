package com.spikeify.converters;

import com.spikeify.Converter;

public class ByteConverter implements Converter<Byte, Long> {

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
