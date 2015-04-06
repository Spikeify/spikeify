package com.spikeify;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;

public class FieldMapper<PROP_TYPE> {

	public FieldMapper(String propName, Class<PROP_TYPE> propType, Converter converter, Field field) {
		this.propName = propName;
		this.propType = propType;
		this.converter = converter;
		this.field = field;

		field.setAccessible(true);
		this.field = field;
		try {
			this.getter = MethodHandles.lookup().unreflectGetter(field);
			this.setter = MethodHandles.lookup().unreflectSetter(field);
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException(e);
		}
	}

	public PROP_TYPE getPropertyValue(Object object) {
		try {
			return (PROP_TYPE) field.get(object);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e); //todo nicer error
		}
	}

	public void setFieldValue(Object targetObject, PROP_TYPE propertyValue) {
		try {
			field.set(targetObject, propertyValue);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e); //todo nicer error
		}
	}

	public String propName;
	public Class propType;

	public Converter converter;

	public Field field;
	public MethodHandle getter;
	public MethodHandle setter;

}
