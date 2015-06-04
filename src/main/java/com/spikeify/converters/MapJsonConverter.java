package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.NoArgClassConstructor;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MapJsonConverter implements Converter<Map, Map> {

	private final Constructor<? extends Map> classConstructor;
	private JsonConverter valueConverter;

	@SuppressWarnings("unchecked")
	public MapJsonConverter(Class<? extends Map> mapType, Class valueType) {
		this.valueConverter = new JsonConverter(valueType);

		if (mapType.equals(Map.class)) {
			mapType = HashMap.class;
		}
		classConstructor = NoArgClassConstructor.getNoArgConstructor(mapType);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map fromProperty(Map propertyMap) {
		if (propertyMap == null) {
			return null;
		}
		Map fieldMap = NoArgClassConstructor.newInstance(classConstructor);
		Set<Map.Entry> entrySet = propertyMap.entrySet();
		for (Map.Entry entry : entrySet) {
			if(entry.getValue() instanceof String){
				// value is String, treat it as JSON
				fieldMap.put(entry.getKey(), valueConverter.fromProperty((String) entry.getValue()));
			} else {
				// value is already a java object
				fieldMap.put(entry.getKey(), entry.getValue());
			}
		}
		return fieldMap;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map fromField(Map fieldMap) {
		Map<Object, String> propertyMap = new HashMap<>(fieldMap.size());
		Set<Map.Entry> entrySet = fieldMap.entrySet();
		for (Map.Entry entry : entrySet) {
			propertyMap.put(entry.getKey(), valueConverter.fromField(entry.getValue()));
		}
		return propertyMap;
	}
}
