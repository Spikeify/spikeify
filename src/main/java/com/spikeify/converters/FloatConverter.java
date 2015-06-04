package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Field;

public class FloatConverter implements Converter<Float, Long>, ConverterFactory {

	@Override
	public Converter init(Field field) {
		return this;
	}

	public boolean canConvert(Class type) {
		return Float.class.isAssignableFrom(type) || float.class.isAssignableFrom(type);
	}

	public Float fromProperty(Long property) {
		return property == null ? null : (float) Double.longBitsToDouble(property);
	}

	public Long fromField(Float fieldValue) {
		return Double.doubleToLongBits(fieldValue);
	}

}
