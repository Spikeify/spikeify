package com.spikeify.converters;

import com.spikeify.Converter;

import java.util.Map;

public class MapConverter implements Converter<Map, Map> {

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
