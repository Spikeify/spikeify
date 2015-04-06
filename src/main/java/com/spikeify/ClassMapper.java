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

	public ClassMapper(Class<TYPE> clazz) {
		this.type = clazz;

		Record recordAnnotation = clazz.getAnnotation(Record.class);
		if (recordAnnotation == null) {
			throw new IllegalStateException("Missing @Record annotation on mapped class " + clazz.getName());
		}
		this.setName = "".equals(recordAnnotation.setName()) ? clazz.getSimpleName() : recordAnnotation.setName();
		this.namespace = "".equals(recordAnnotation.namespace()) ? null : recordAnnotation.namespace();

		mappers = MapperUtils.getFieldMappers(clazz);
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

}
