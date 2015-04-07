package com.spikeify.converters;

import com.spikeify.Converter;

public class StringConverter implements Converter<String, String> {

	public boolean canConvert(Class type) {
		return String.class.isAssignableFrom(type);
	}

	@Override
	public String fromProperty(String property) {
		return property;
	}

	@Override
	public String fromField(String fieldValue) {
		return fieldValue;
	}


}
