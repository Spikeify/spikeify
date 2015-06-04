package com.spikeify;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;

@SuppressWarnings({"unchecked", "WeakerAccess"})
public class FieldMapper<F, P> {

	public FieldMapper(String propName, Converter<F, P> converter, Field field) {
		this.propName = propName;
		this.converter = converter;
		this.field = field;

		field.setAccessible(true);
		this.field = field;
		try {
			this.getter = MethodHandles.lookup().unreflectGetter(field);
			this.setter = MethodHandles.lookup().unreflectSetter(field);
		} catch (ReflectiveOperationException e) {
			throw new SpikeifyError(e);
		}
	}

	public P getPropertyValue(Object object) {
		try {
			F fieldValue = (F) field.get(object);
			if (fieldValue == null) {
				return null;
			}
			return converter.fromField(fieldValue);
		} catch (IllegalAccessException e) {
			throw new SpikeifyError(e); //todo nicer error
		}
	}

	public void setFieldValue(Object targetObject, P propertyValue) {
		try {
			F value = converter.fromProperty(propertyValue);
			if (!(value == null && field.getType().isPrimitive())) { // do not set value if primitive type, leave it default
				field.set(targetObject, value);
			}
		} catch (IllegalAccessException e) {
			throw new SpikeifyError(e); //todo nicer error
		}
	}

	public final String propName;
	//	public Class<P> propType;
	public final Converter<F, P> converter;

	//	public Class<F> fieldType;
	public Field field;
	public MethodHandle getter;
	public MethodHandle setter;

}
