package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;
import com.spikeify.TypeUtils;
import com.spikeify.annotations.AsJson;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Map;

public class MapConverterFactory implements ConverterFactory {

	@Override
	public Converter init(Field field) {
		Type valueType = TypeUtils.getMapValueType(field);

		if (valueType != null && valueType instanceof Class) {
			Class classType = (Class) valueType;
			if (classType.isAnnotationPresent(AsJson.class)) {
				Class fieldType = field.getType();
				return new MapJsonConverter(fieldType, classType);
			}
		}

		return new MapConverter();
	}

	public boolean canConvert(Class type) {
		return Map.class.isAssignableFrom(type);
	}

}
