package com.spikeify.aerospike;

import com.aerospike.client.Record;
import com.spikeify.Converter;

public class ShortConverter implements Converter<Short, Integer> {

	public boolean canConvert(Class type) {
		return Short.class.isAssignableFrom(type);
	}

	public Short fromProperty(Integer property) {
		return property.shortValue();
	}

	public Integer fromField(Short fieldValue) {
		return Integer.valueOf(fieldValue);
	}

}
