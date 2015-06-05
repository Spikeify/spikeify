package com.spikeify;

import com.aerospike.client.Key;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class ClassMapper<TYPE> {

	private final Map<String/** field name**/, FieldMapper> mappers;

	private final Class<TYPE> type;
	private final String classSetName;
	private final String classNamespace;

	private final FieldMapper<Integer, Integer> generationFieldMapper;
	private final FieldMapper<Long, Long> expirationFieldMapper;
	private final FieldMapper<String, String> namespaceFieldMapper;
	private final FieldMapper<String, String> setNameFieldMapper;
	private final FieldMapper userKeyFieldMapper;
	private final FieldMapper anyPropertyMapper;

	public ClassMapper(Class<TYPE> clazz) {
		Map<String, FieldMapper> fieldMappers;
		this.type = clazz;

		// parse @Namespace class annotation
		Namespace namespaceClassAnnotation = clazz.getAnnotation(Namespace.class);
		classNamespace = namespaceClassAnnotation != null ? namespaceClassAnnotation.value() : null;

		// parse @SetName class annotation
		SetName setNameAnnotation = clazz.getAnnotation(SetName.class);
		classSetName = setNameAnnotation != null ? setNameAnnotation.value() : null;

		fieldMappers = MapperUtils.getFieldMappers(clazz);
		fieldMappers.putAll(MapperUtils.getJsonMappers(clazz));
		mappers = fieldMappers;

		generationFieldMapper = MapperUtils.getGenerationFieldMapper(clazz);
		expirationFieldMapper = MapperUtils.getExpirationFieldMapper(clazz);
		namespaceFieldMapper = MapperUtils.getNamespaceFieldMapper(clazz);
		setNameFieldMapper = MapperUtils.getSetNameFieldMapper(clazz);
		userKeyFieldMapper = MapperUtils.getUserKeyFieldMapper(clazz);
		anyPropertyMapper = MapperUtils.getAnyFieldMapper(clazz);
	}

	public Class<TYPE> getType() {
		return type;
	}

	public ObjectMetadata getRequiredMetadata(Object target, String defaultNamespace) {
		Class type = target.getClass();
		ObjectMetadata metadata = new ObjectMetadata();

		// acquire UserKey
		if (userKeyFieldMapper == null) {
			throw new SpikeifyError("Class " + type.getName() + " is missing a field with @UserKey annotation.");
		}
		Object userKeyObj = userKeyFieldMapper.getPropertyValue(target);
		if (userKeyObj instanceof String) {
			metadata.userKeyString = (String) userKeyObj;
		} else if (userKeyObj instanceof Long) {
			metadata.userKeyLong = (Long) userKeyObj;
		} else {
			throw new SpikeifyError("@UserKey annotation can only be used on fields of type: String, Long or long." +
					" Field " + type.getName() + "$" + userKeyFieldMapper.field.getName() + " type is " + userKeyFieldMapper.field.getType().getName());
		}

		// acquire Namespace in the following order
		// 1. use @Namespace on a field or
		// 2. use @Namespace on class or
		// 3. use default namespace
		String fieldNamespace = namespaceFieldMapper != null ? namespaceFieldMapper.getPropertyValue(target) : null;
		metadata.namespace = fieldNamespace != null ? fieldNamespace :
				(classNamespace != null ? classNamespace : defaultNamespace);
		// namespace still not available
		if (metadata.namespace == null) {
			throw new SpikeifyError("Error: namespace could not be inferred from class/field annotations, " +
					"for class " + type.getName() +
					", nor is default namespace available.");
		}

		// acquire @SetName in the following order
		// 1. use @SetName on a field or
		// 2. use @SetName on class or
		// 3. Use Class simple name
		String fieldSetName = setNameFieldMapper != null ? setNameFieldMapper.getPropertyValue(target) : null;
		metadata.setName = fieldSetName != null ? fieldSetName :
				(classSetName != null ? classSetName : type.getSimpleName());

		// acquire @Expires
		metadata.expires = expirationFieldMapper != null ? expirationFieldMapper.getPropertyValue(target) : null;

		// acquire @Generation
		metadata.generation = generationFieldMapper != null ? generationFieldMapper.getPropertyValue(target) : null;

		return metadata;
	}

	public String getSetName() {
		return classSetName != null ? classSetName : type.getSimpleName();
	}

	public String getNamespace() {
		return classNamespace;
	}

	public Map<String, Object> getProperties(TYPE object) {

		Map<String, Object> props = new HashMap<>(mappers.size());
		for (FieldMapper fieldMapper : mappers.values()) {
			Object propertyValue = fieldMapper.getPropertyValue(object);
			props.put(fieldMapper.propName, propertyValue);
		}

		// find unmapped properties
		if (anyPropertyMapper != null) {
			Map<String, Object> unmappedProperties = (Map<String, Object>) anyPropertyMapper.getPropertyValue(object);
			for (String propName : unmappedProperties.keySet()) {
				props.put(propName, unmappedProperties.get(propName));
			}
		}

		return props;
	}
	public FieldMapper getFieldMapper(String fieldName) {
		return mappers.get(fieldName);
	}

	public void setFieldValues(TYPE object, Map<String, Object> properties) {

		// create a copy
		Map<String, Object> mappedProps = new HashMap<>(properties);

		for (FieldMapper fieldMapper : mappers.values()) {
			Object prop = mappedProps.get(fieldMapper.propName);
			mappedProps.remove(fieldMapper.propName);
			fieldMapper.setFieldValue(object, prop);
		}

		// at this point mappedProps should only contain unmapped properties
		if (anyPropertyMapper != null) {
			anyPropertyMapper.setFieldValue(object, mappedProps);
		}
	}

	private long getJavaExpiration(int recordExpiration) {
		long javaExpiration;

		// Aerospike expiry settings are messed up: ypu put in -1 and get back 0
		if (recordExpiration == 0) {
			javaExpiration = -1; // default expiration setting: -1 - no expiration set
		} else {
			// convert record expiration time (seconds from 01/01/2010 0:0:0 GMT)
			// to java epoch time in milliseconds
			javaExpiration = 1000l * (1262304000l + recordExpiration);
		}
		return javaExpiration;
	}

	private int getRecordExpiration(long javaExpiration) {
		int recordExpiration;

		if (javaExpiration == 0 || javaExpiration == -1) {
			recordExpiration = (int) javaExpiration; // default expiration settings: 0 - server default expiration, -1 - no expiration
		} else {
			// convert record expiration time (seconds from 01/01/2010 0:0:0 GMT)
			// to java epoch time in milliseconds
			long now = System.currentTimeMillis();
			recordExpiration = (int) ((javaExpiration - now) / 1000l);
		}
		return recordExpiration;
	}

	public void setMetaFieldValues(Object object, String namespace, String setName, int generation, int recordExpiration) {

		if (generationFieldMapper != null) {
			generationFieldMapper.setFieldValue(object, generation);
		}
		if (expirationFieldMapper != null) {
			expirationFieldMapper.setFieldValue(object, getJavaExpiration(recordExpiration));
		}
		if (namespaceFieldMapper != null) {
			namespaceFieldMapper.setFieldValue(object, namespace);
		}
		if (setNameFieldMapper != null) {
			setNameFieldMapper.setFieldValue(object, setName);
		}
	}

	public Integer getRecordExpiration(TYPE object) {
		if (expirationFieldMapper == null) {
			return null;
		}
		return getRecordExpiration(expirationFieldMapper.getPropertyValue(object));
	}

	/**
	 * Gets a value of the field marked with @Generation
	 *
	 * @param object Object.
	 * @return null if no @Generation marked field exists, 0 if this is a new object, otherwise an integer value
	 */
	public Integer getGeneration(TYPE object) {
		if (generationFieldMapper == null) {
			return null;
		}
		Integer generation = generationFieldMapper.getPropertyValue(object);
		return generation == null ? 0 : generation;  // default generation value for new objects is 0
	}

	public void setUserKey(TYPE object, String userKey) {
		if (userKeyFieldMapper != null) {
			if (!userKeyFieldMapper.field.getType().isAssignableFrom((userKey.getClass()))) {
				throw new SpikeifyError("Key type mismatch: @UserKey field '" +
						userKeyFieldMapper.field.getDeclaringClass().getName() + "#" + userKeyFieldMapper.field.getName() +
						"' has type '" + userKeyFieldMapper.field.getType() + "', while key has type 'String'."
				);
			}
			userKeyFieldMapper.setFieldValue(object, userKey);
		}
	}

	public void setUserKey(TYPE object, Long userKey) {
		if (userKeyFieldMapper != null) {
			if (!userKeyFieldMapper.field.getType().isAssignableFrom((userKey.getClass()))) {
				throw new SpikeifyError("Key type mismatch: @UserKey field '" +
						userKeyFieldMapper.field.getDeclaringClass().getName() + "#" + userKeyFieldMapper.field.getName() +
						"' has type '" + userKeyFieldMapper.field.getType() + "', while key has type 'Long'."
				);
			}
			userKeyFieldMapper.setFieldValue(object, userKey);
		}
	}


	public String getBinName(String fieldName) {
		FieldMapper fieldMapper = mappers.get(fieldName);
		return MapperUtils.getBinName(fieldMapper.field);
	}

	public void checkKeyType(Key key) {
		try {
			userKeyFieldMapper.converter.fromProperty(key.userKey.getObject());
		} catch (ClassCastException e) {
			throw new SpikeifyError("Mismatched key type: provided " + key.userKey.getObject().getClass().getName() +
					" key can not be mapped to " + userKeyFieldMapper.field.getType() + " ("
					+ userKeyFieldMapper.field.getDeclaringClass().getName() + "." + userKeyFieldMapper.field.getName() + ")");
		}
	}
}
