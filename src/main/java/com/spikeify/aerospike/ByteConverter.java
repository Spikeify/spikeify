package com.spikeify.aerospike;

import com.spikeify.Converter;

import java.lang.reflect.Type;

public class ByteConverter implements Converter<Byte, Integer> {

	public boolean canConvert(Class type) {
		return Byte.class.isAssignableFrom(type) || byte.class.isAssignableFrom(type);
	}

	public Byte fromProperty(Integer property) {
		return property.byteValue();
	}

	public Integer fromField(Byte fieldValue) {
		return Integer.valueOf(fieldValue);
	}

}
