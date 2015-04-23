package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

public class JsonConverterFactory implements ConverterFactory {


	@Override
	public Converter init(Class type) {
		return new JsonConverter(type);
	}

	public boolean canConvert(Class type) {
		return true; // Jackson should be able to convert any Object to JSON
	}


}
