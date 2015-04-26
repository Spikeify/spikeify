package com.spikeify;

import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.Record;
import com.spikeify.annotations.SetName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClassMapper<TYPE> {

	private final List<FieldMapper> mappers;

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
		this.type = clazz;

		// @Record annotation os mandatory
		Record recordAnnotation = clazz.getAnnotation(Record.class);
		if (recordAnnotation == null) {
			throw new SpikeifyError("Missing @Record annotation on mapped class " + clazz.getName());
		}

		// parse @Namespace class annotation
		Namespace namespaceClassAnnotation = clazz.getAnnotation(Namespace.class);
		classNamespace = namespaceClassAnnotation != null ? namespaceClassAnnotation.value() : null;

		// parse @SetName class annotation
		SetName setNameAnnotation = clazz.getAnnotation(SetName.class);
		classSetName = setNameAnnotation != null ? setNameAnnotation.value() : null;

		mappers = MapperUtils.getFieldMappers(clazz);

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

		Map<String, Object> props = new HashMap<String, Object>(mappers.size());
		for (FieldMapper fieldMapper : mappers) {
			Object propertyValue = fieldMapper.getPropertyValue(object);
			if (propertyValue != null) {
				props.put(fieldMapper.propName, propertyValue);
			}
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

	public void setFieldValues(TYPE object, Map<String, Object> properties) {

		// create a copy
		Map<String, Object> mappedProps = new HashMap<>(properties);

		for (FieldMapper fieldMapper : mappers) {
			Object prop = mappedProps.get(fieldMapper.propName);
			if (prop != null) {
				mappedProps.remove(fieldMapper.propName);
				fieldMapper.setFieldValue(object, prop);
			}
		}

		// at this point mappedProps should only contain unmapped properties
		if (anyPropertyMapper != null) {
			anyPropertyMapper.setFieldValue(object, mappedProps);
		}
	}

	public void setMetaFieldValues(Object object, String namespace, String setName, int generation, long expiration) {

		if (generationFieldMapper != null) {
			generationFieldMapper.setFieldValue(object, generation);
		}
		if (expirationFieldMapper != null) {
			expirationFieldMapper.setFieldValue(object, expiration);
		}
		if (namespaceFieldMapper != null) {
			namespaceFieldMapper.setFieldValue(object, namespace);
		}
		if (setNameFieldMapper != null) {
			setNameFieldMapper.setFieldValue(object, setName);
		}
	}

	public Long getExpiration(TYPE object) {
		if (expirationFieldMapper == null) {
			return null;
		}
		return expirationFieldMapper.getPropertyValue(object);
	}

	public Integer getGeneration(TYPE object) {
		if (generationFieldMapper == null) {
			return null;
		}
		return generationFieldMapper.getPropertyValue(object);
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

	public void setUserKey(Object object, Long userKey) {
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


}
