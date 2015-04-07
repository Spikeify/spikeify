package com.spikeify;

import com.spikeify.annotations.Record;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClassMapper<TYPE> {

	private final List<FieldMapper> mappers;

	private final Type type;
	private final String setName;
	private final String namespace;

	private final FieldMapper<Integer, Integer> generationFieldMapper;
	private final FieldMapper<Long, Long> expirationFieldMapper;
	private final FieldMapper<String, String> namespaceFieldMapper;
	private final FieldMapper<String, String> setNameFieldMapper;

	public ClassMapper(Class<TYPE> clazz) {
		this.type = clazz;

		Record recordAnnotation = clazz.getAnnotation(Record.class);
		if (recordAnnotation == null) {
			throw new IllegalStateException("Missing @Record annotation on mapped class " + clazz.getName());
		}
		this.setName = "".equals(recordAnnotation.setName()) ? clazz.getSimpleName() : recordAnnotation.setName();
		this.namespace = "".equals(recordAnnotation.namespace()) ? null : recordAnnotation.namespace();

		mappers = MapperUtils.getFieldMappers(clazz);

		generationFieldMapper = MapperUtils.getGenerationFieldMapper(clazz);
		expirationFieldMapper = MapperUtils.getExpirationFieldMapper(clazz);
		namespaceFieldMapper = MapperUtils.getNamespaceFieldMapper(clazz);
		setNameFieldMapper = MapperUtils.getSetNameFieldMapper(clazz);
	}

	public Type getType() {
		return type;
	}

	public String getSetName() {
		return setName;
	}

	public String getNamespace() {
		return namespace;
	}

	public Map<String, Object> getProperties(TYPE object) {

		Map<String, Object> props = new HashMap<String, Object>(mappers.size());
		for (FieldMapper fieldMapper : mappers) {
			props.put(fieldMapper.propName, fieldMapper.getPropertyValue(object));
		}
		return props;
	}

	public void setFieldValues(TYPE object, Map<String, Object> properties) {

		for (FieldMapper fieldMapper : mappers) {
			fieldMapper.setFieldValue(object, properties.get(fieldMapper.propName));
		}
	}

	public void setMetaFieldValues(TYPE object, String namespace, String setName, int generation, long expiration) {

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

	public Long getId(TYPE object) {
		if (expirationFieldMapper == null) {
			return null;
		}
		return expirationFieldMapper.getPropertyValue(object);
	}
}
