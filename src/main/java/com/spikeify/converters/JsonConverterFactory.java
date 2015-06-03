package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Field;

public class JsonConverterFactory implements ConverterFactory {

	@Override
	public Converter init(Field field) {
		return new JsonConverter(field);
	}

	public boolean canConvert(Class type) {
		return true; // Jackson should be able to convert any Object to JSON
	}


}
