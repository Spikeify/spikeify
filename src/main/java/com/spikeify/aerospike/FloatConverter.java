package com.spikeify.aerospike;

import com.spikeify.Converter;

public class FloatConverter implements Converter<Float, Long> {

	public boolean canConvert(Class type) {
		return Float.class.isAssignableFrom(type) || float.class.isAssignableFrom(type);
	}

	public Float fromProperty(Long property) {
		return (float) Double.longBitsToDouble(property);
	}

	public Long fromField(Float fieldValue) {
		return Double.doubleToLongBits(fieldValue);
	}

}
