package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;
import com.spikeify.TypeUtils;
import com.spikeify.annotations.AsJson;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;

public class ListConverterFactory implements ConverterFactory {

	@SuppressWarnings("unchecked")
	@Override
	public Converter init(Field field) {

		Type valueType = TypeUtils.getListValueType(field);

		if (valueType instanceof Class) {
			Class classType = (Class) valueType;
			if (classType.isAnnotationPresent(AsJson.class)) {
				Class fieldType = field.getType();
				return new ListJsonConverter(fieldType, classType);
			}
		}

		return new ListConverter();
	}

	@Override
	public boolean canConvert(Class type) {
		return List.class.isAssignableFrom(type);
	}

}
