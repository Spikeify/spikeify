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

	/**
	 * Performs common mapping task when loading an entity ... used in loaders
	 * see {@link com.spikeify.commands.SingleLoader} {@link com.spikeify.commands.MultiLoader} {@link com.spikeify.commands.ScanLoader}
	 * @param mapper to be used
	 * @param key record key
	 * @param record record value
	 * @param object object holding data
	 */
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
