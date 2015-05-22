package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Field;

public class PassThroughConverter implements Converter<Object, Object>, ConverterFactory {

	@Override
	public Converter init(Field field) {
		return this;
	}

	/**
	 * Designed to only be used manually, not via class/field introspection
	 * @param type The type to check.
	 * @return Always returns false, so it can not be discovered via field mapping process
	 */
	public boolean canConvert(Class type) {
		return false;  // always returns false, so it can not be discovered via field mapping process
	}

	public Object fromProperty(Object property) {
		return property;
	}

	public Object fromField(Object fieldValue) {
		return fieldValue;
	}

}
