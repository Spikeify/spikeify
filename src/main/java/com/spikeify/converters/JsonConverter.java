package com.spikeify.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikeify.Converter;
import com.spikeify.SpikeifyError;

import java.io.IOException;

public class JsonConverter implements Converter<Object, String> {

	private static final ThreadLocal<ObjectMapper> tlObjectMapper = new ThreadLocal<ObjectMapper>() {
		@Override
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		}
	};

	private Class type;

	public JsonConverter(Class type) {
		this.type = type;
	}

	@Override
	public Object fromProperty(String property) {
		try {
			return tlObjectMapper.get().readValue(property, type);
		} catch (IOException e) {
			throw new SpikeifyError("Error deserializing from JSON: ", e);
		}
	}

	@Override
	public String fromField(Object fieldValue) {
		try {
			return tlObjectMapper.get().writeValueAsString(fieldValue);
		} catch (JsonProcessingException e) {
			throw new SpikeifyError("Error serializing to JSON: ", e);
		}
	}


}
