package com.spikeify;

import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class ClassMapper<TYPE> {

	private final Map<String/** field name **/, FieldMapper> mappers;
	private final Map<String/** field name **/, Class<? extends BigDatatypeWrapper>> ldtMappers;

	private final Class<TYPE> type;
	private final String classSetName;
	private final String classNamespace;

	private final FieldMapper<Integer, Integer> generationFieldMapper;
	private final FieldMapper<Long, Long> expirationFieldMapper;
	private final FieldMapper<Long, Long> ttlFieldMapper;
	private final FieldMapper<String, String> namespaceFieldMapper;
	private final FieldMapper<String, String> setNameFieldMapper;
	private final FieldMapper userKeyFieldMapper;
	private final FieldMapper anyPropertyMapper;

	public ClassMapper(Class<TYPE> clazz) {
		this.type = clazz;

		// parse @Namespace class annotation
		Namespace namespaceClassAnnotation = clazz.getAnnotation(Namespace.class);
		classNamespace = namespaceClassAnnotation != null ? namespaceClassAnnotation.value() : null;

		// parse @SetName class annotation
		SetName setNameAnnotation = clazz.getAnnotation(SetName.class);
		classSetName = setNameAnnotation != null ? setNameAnnotation.value() : null;

		Map<String, FieldMapper> fieldMappers = MapperUtils.getFieldMappers(clazz);
//		fieldMappers.putAll(MapperUtils.getJsonMappers(clazz));
		mappers = fieldMappers;

		ldtMappers = MapperUtils.getLDTClasses(clazz);

		generationFieldMapper = MapperUtils.getGenerationFieldMapper(clazz);
		expirationFieldMapper = MapperUtils.getExpirationFieldMapper(clazz);
		ttlFieldMapper = MapperUtils.getTtlFieldMapper(clazz);
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
		if (userKeyObj == null) {
			throw new SpikeifyError("Field with @UserKey annotation cannot be null" +
					" Field " + type.getName() + "$" + userKeyFieldMapper.field.getName() + " type is " + userKeyFieldMapper.field.getType().getName() + ", value: null");
		} else if (userKeyObj instanceof String) {
			metadata.userKeyString = (String) userKeyObj;
			if (metadata.userKeyString.isEmpty()) {
				throw new SpikeifyError("Field with @UserKey annotation and with type of String cannot be empty" +
						" Field " + type.getName() + "$" + userKeyFieldMapper.field.getName() + " type is " + userKeyFieldMapper.field.getType().getName() + ", value: ''");

			}
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
		// acquire @TimeToLive
		metadata.ttl = ttlFieldMapper != null ? ttlFieldMapper.getPropertyValue(target) : null;

		// acquire @Generation
		metadata.generation = generationFieldMapper != null ? generationFieldMapper.getPropertyValue(target) : null;

		return metadata;
	}

	/**
	 * @return returns set name according to class type or setName annotation
	 */
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
			props.put(fieldMapper.binName, propertyValue);
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
			Object prop = mappedProps.get(fieldMapper.binName);
			mappedProps.remove(fieldMapper.binName);
			if (prop != null) {
				fieldMapper.setFieldValue(object, prop);
			}
		}

		// at this point mappedProps should only contain unmapped properties
		if (anyPropertyMapper != null) {
			anyPropertyMapper.setFieldValue(object, mappedProps);
		}
	}


	public void setBigDatatypeFields(Object object, IAsyncClient client, Key key) {

		if (!(client instanceof AsyncClient)) {
			return;  // only real client can be used, mocks do not support LDTs
		}

		for (Map.Entry<String, Class<? extends BigDatatypeWrapper>> entry : ldtMappers.entrySet()) {
			Field field = null;
			try {
				field = object.getClass().getDeclaredField(entry.getKey()); // to see all fields not just public ones
			} catch (NoSuchFieldException e) {
				// should not happen
				throw new SpikeifyError("Field '" + entry.getKey() + "' on class " + object.getClass() + " not found!");
			}

			try {
				field.setAccessible(true); // to allow setting private fields
				BigDatatypeWrapper wrapper = (BigDatatypeWrapper) field.get(object);
				if (wrapper == null || !wrapper.isInitialized()) {
					wrapper = (new NoArgClassConstructor()).construct(entry.getValue());
					wrapper.init(client, key, MapperUtils.getBinName(field), field);
				}
				field.set(object, wrapper);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Translates bin names/values into field names/values.
	 *
	 * @param properties map of field properties
	 * @return mapped properties
	 */
	public Map<String, Object> getFieldValues(Map<String, Object> properties) {

		Map<String, Object> fieldValues = new HashMap<>();

		for (FieldMapper fieldMapper : mappers.values()) {
			if (properties.containsKey(fieldMapper.binName)) {
				Object propValue = properties.get(fieldMapper.binName);
				fieldValues.put(fieldMapper.field.getName(), fieldMapper.getFieldValue(propValue));
			}
		}

		return fieldValues;
	}


	public void setMetaFieldValues(Object object, String namespace, String setName, int generation, int recordExpiration) {

		if (generationFieldMapper != null) {
			generationFieldMapper.setFieldValue(object, generation);
		}
		if (expirationFieldMapper != null) {
			expirationFieldMapper.setFieldValue(object, ExpirationUtils.getExpirationMillisAbs(recordExpiration));
		}
		if (ttlFieldMapper != null) {
			ttlFieldMapper.setFieldValue(object, ExpirationUtils.getExpirationMillisRelative(recordExpiration));
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
		return ExpirationUtils.getRecordExpiration(expirationFieldMapper.getPropertyValue(object));
	}

	public Long getRecordTtl(TYPE object) {
		if (ttlFieldMapper == null) {
			return null;
		}
		return ttlFieldMapper.getPropertyValue(object);
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
			if (!String.class.isAssignableFrom(userKeyFieldMapper.field.getType())) {
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
			if (!long.class.isAssignableFrom(userKeyFieldMapper.field.getType()) && // UserKey could be a long but given "userKey" will always be Long
					!Long.class.isAssignableFrom(userKeyFieldMapper.field.getType())) {
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
