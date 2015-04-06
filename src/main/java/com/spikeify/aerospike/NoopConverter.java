package com.spikeify.aerospike;

import com.spikeify.Converter;

public class NoopConverter<TYPE> implements Converter<TYPE, TYPE> {

	public boolean canConvert(Class type) {
		return String.class.isAssignableFrom(type) ||
				Integer.class.isAssignableFrom(type) ||
				int.class.isAssignableFrom(type) ||
				Long.class.isAssignableFrom(type) ||
				long.class.isAssignableFrom(type) ||
				Float.class.isAssignableFrom(type) ||
				float.class.isAssignableFrom(type) ||
				Double.class.isAssignableFrom(type) ||
				double.class.isAssignableFrom(type);
	}

	public TYPE fromProperty(TYPE property) {
		return property;
	}

	public TYPE fromField(TYPE fieldValue) {
		return fieldValue;
	}

}
