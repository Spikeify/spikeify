package com.spikeify;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClassMapper<TYPE> {

	private final List<FieldMapper> mappers;

	public ClassMapper(Class<TYPE> clazz) {
		this.type = clazz;

		mappers = MapperUtils.getFieldMappers(clazz);

//		for (FieldMapper mapper : MapperUtils.getFieldMappers(clazz)) {
//			fieldMappings.put(mapper.field, mapper);
//			propertyMappings.put(mapper.propName, mapper);
//		}

	}

	private Type type;

//	private Map<Field, FieldMapper> fieldMappings = new HashMap<Field, FieldMapper>();

//	private Map<String, FieldMapper> propertyMappings = new HashMap<String, FieldMapper>();

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
