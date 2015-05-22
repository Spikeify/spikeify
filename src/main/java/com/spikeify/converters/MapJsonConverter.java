package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.NoArgClassConstructor;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public class MapJsonConverter<T> implements Converter<Map<Object, T>, Map<Object, String>> {

	private final Constructor<? extends Map> classConstructor;
	private JsonConverter<T> valueConverter;

	public MapJsonConverter(Class<? extends Map> mapType, Class<T> valueType) {
		this.valueConverter = new JsonConverter(valueType);

		if (mapType.equals(Map.class)) {
			mapType = HashMap.class;
		}
		classConstructor = NoArgClassConstructor.getNoArgConstructor(mapType);
	}

	@Override
	public Map<Object, T> fromProperty(Map<Object, String> propertyMap) {
		Map<Object, T> fieldMap = NoArgClassConstructor.newInstance(classConstructor);
		for (Map.Entry<Object, String> entry : propertyMap.entrySet()) {
			fieldMap.put(entry.getKey(), valueConverter.fromProperty(entry.getValue()));
		}
		return fieldMap;
	}

	@Override
	public Map<Object, String> fromField(Map<Object, T> fieldMap) {
		Map<Object, String> propertyMap = new HashMap<>(fieldMap.size());
		for (Map.Entry<Object, T> entry : fieldMap.entrySet()) {
			propertyMap.put(entry.getKey(), valueConverter.fromField(entry.getValue()));
		}
		return propertyMap;
	}
}
