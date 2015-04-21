package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Type;
import java.util.List;

public class ListConverter implements Converter<List, List> , ConverterFactory {

	@Override
	public Converter init(Type type) {
		return this;
	}

	public boolean canConvert(Class type) {
		return List.class.isAssignableFrom(type);
	}

	public List fromProperty(List list) {
		return list;
	}

	public List fromField(List fieldValue) {
		return fieldValue;
	}

}
