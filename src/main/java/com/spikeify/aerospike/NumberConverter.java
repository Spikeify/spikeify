//package com.spikeify.aerospike;
//
//import com.spikeify.Converter;
//
//public class NumberConverter<F> implements Converter<F, Long> {
//
//	public boolean canConvert(Class type) {
//		return Integer.class.isAssignableFrom(type) ||
//				int.class.isAssignableFrom(type) ||
//				Long.class.isAssignableFrom(type) ||
//				long.class.isAssignableFrom(type) ||
//				Float.class.isAssignableFrom(type) ||
//				float.class.isAssignableFrom(type) ||
//				Double.class.isAssignableFrom(type) ||
//				double.class.isAssignableFrom(type);
//	}
//
//	@Override
//	public F fromProperty(Long property) {
//		return property;
//	}
//
//	@Override
//	public Long fromField(F fieldValue) {
//		return null;
//	}
//
//
//}
