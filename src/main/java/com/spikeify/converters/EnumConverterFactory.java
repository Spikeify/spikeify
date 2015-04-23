package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EnumConverterFactory implements ConverterFactory {

	private static Map<Type, EnumConverter> cachedConverters = new ConcurrentHashMap<>();

	@Override
	public Converter init(Class type) {
		EnumConverter converter = cachedConverters.get(type);
		if (converter == null) {
			converter = new EnumConverter(type);
			cachedConverters.put(type, converter);
		}
		return converter;
	}

	public boolean canConvert(Class type) {
		return Enum.class.isAssignableFrom(type);
	}


}
