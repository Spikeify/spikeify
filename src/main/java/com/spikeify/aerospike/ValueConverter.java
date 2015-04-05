package com.spikeify.aerospike;

import com.spikeify.Converter;

public class ValueConverter<TYPE> implements Converter<TYPE, TYPE> {

	public boolean canConvert(Class type) {
		return
				Integer.class.isAssignableFrom(type) ||
				Long.class.isAssignableFrom(type) ||
				Float.class.isAssignableFrom(type) ||
				Double.class.isAssignableFrom(type) ||
				String.class.isAssignableFrom(type);
	}

	public TYPE fromProperty(TYPE property) {
		return property;
	}

	public TYPE fromField(TYPE fieldValue) {
		return fieldValue;
	}

}
