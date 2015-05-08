package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.util.Set;

@SuppressWarnings("unchecked")
public class SetConverterFactory implements ConverterFactory {


	@Override
	public Converter init(Class type) {
		return new SetConverter<>(type);
	}

	@Override
	public boolean canConvert(Class type) {
		return Set.class.isAssignableFrom(type);
	}
}
