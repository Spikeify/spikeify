package com.spikeify.aerospike;

import com.spikeify.Converter;

import java.util.Date;

public class DateConverter implements Converter<Date, Long> {

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
