package com.spikeify;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.ParticleType;

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

	public static void map(ClassMapper mapper, Key key, Record record, Object object) {
		// set UserKey field
		switch (key.userKey.getType()) {
			case ParticleType.STRING:
				mapper.setUserKey(object, key.userKey.toString());
				break;
			case ParticleType.INTEGER:
				mapper.setUserKey(object, key.userKey.toLong());
				break;
		}

		// set meta-fields on the entity: @Namespace, @SetName, @Expiration..
		mapper.setMetaFieldValues(object, key.namespace, key.setName, record.generation, record.expiration);

		// set field values
		mapper.setFieldValues(object, record.bins);
	}
}
