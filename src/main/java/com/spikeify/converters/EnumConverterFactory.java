package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EnumConverterFactory implements ConverterFactory {

	private static final Map<Type, EnumConverter> cachedConverters = new ConcurrentHashMap<>();

	@Override
	public Converter init(Field field) {
		Type fieldType = field.getType();
		EnumConverter converter = cachedConverters.get(fieldType);
		if (converter == null) {
			converter = new EnumConverter(fieldType);
			cachedConverters.put(fieldType, converter);
		}
		return converter;
	}

	public boolean canConvert(Class type) {
		return Enum.class.isAssignableFrom(type);
	}


}
