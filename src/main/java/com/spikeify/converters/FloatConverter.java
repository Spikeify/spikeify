package com.spikeify.converters;

import com.aerospike.client.Value;
import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Field;

public class FloatConverter implements Converter<Float, Object>, ConverterFactory {

	@Override
	public Converter init(Field field) {
		return this;
	}

	public boolean canConvert(Class type) {
		return Float.class.isAssignableFrom(type) || float.class.isAssignableFrom(type);
	}

	public Float fromProperty(Object property) {
		if (property == null) {
			return null;
		} else if (property instanceof Float) {
			return (Float) property;
		} else if (property instanceof Double) {
			return ((Double) property).floatValue();
		} else if (property instanceof Long) {
			return (float) Double.longBitsToDouble((Long) property);
		} else {
			throw new IllegalArgumentException("Fields of type 'float' can only be mapped to DB values of Long or Double.");
		}
	}

	public Object fromField(Float fieldValue) {

		// is double supported by the database
		if (Value.UseDoubleType) {
			return fieldValue.doubleValue();  // return Double
		} else {
			return Double.doubleToLongBits(fieldValue); // return Long - the old, pre-3.6.0 way of converting Double to Long
		}
	}

}
