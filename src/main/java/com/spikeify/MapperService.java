package com.spikeify;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MapperService {

	private static Map<Class, ClassMapper> classMappers = new ConcurrentHashMap<Class, ClassMapper>();

	public static <T> ClassMapper<T> getMapper(Class<T> clazz){

		ClassMapper<T> classMapper = classMappers.get(clazz);
		if(classMapper==null){
			classMapper = new ClassMapper<T>(clazz);
			classMappers.put(clazz, classMapper);
		}
		return classMapper;
	}
}
