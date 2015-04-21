package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.lang.reflect.Type;
import java.util.Date;

public class DateConverter implements Converter<Date, Long>, ConverterFactory {

	@Override
	public Converter init(Type type) {
		return this;
	}

	public boolean canConvert(Class type) {
		return Date.class.isAssignableFrom(type);
	}

	public Date fromProperty(Long property) {
		return new Date(property);
	}

	public Long fromField(Date date) {
		return date.getTime();
	}
}
