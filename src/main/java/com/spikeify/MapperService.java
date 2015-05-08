package com.spikeify;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unchecked")
public class MapperService {

	private static final Map<Class, ClassMapper> classMappers = new ConcurrentHashMap<>();

	public static <T> ClassMapper<T> getMapper(Class<T> clazz) {

		ClassMapper<T> classMapper = classMappers.get(clazz);
		if (classMapper == null) {
			classMapper = new ClassMapper<>(clazz);
			classMappers.put(clazz, classMapper);
		}
		return classMapper;
	}
}
