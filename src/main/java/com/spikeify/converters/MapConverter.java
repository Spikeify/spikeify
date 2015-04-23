package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.util.Map;

public class MapConverter implements Converter<Map, Map>, ConverterFactory {

	@Override
	public Converter init(Class type) {
		return this;
	}

	public boolean canConvert(Class type) {
		return Map.class.isAssignableFrom(type);
	}

	public Map fromProperty(Map map) {
		return map;
	}

	public Map fromField(Map fieldValue) {
		return fieldValue;
	}

}
