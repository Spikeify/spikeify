package com.spikeify.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.spikeify.Converter;
import com.spikeify.SpikeifyError;

import java.io.IOException;
import java.lang.reflect.Field;

public class JsonConverter<T> implements Converter<T, String> {

	private static final ThreadLocal<ObjectMapper> tlObjectMapper = new ThreadLocal<ObjectMapper>() {
		@Override
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		}
	};

	private final JavaType type;

	public JsonConverter(Class<T> type) {
		this.type = SimpleType.construct(type);
	}

	public JsonConverter(Field field) {
		this.type = TypeFactory.defaultInstance().constructType(field.getGenericType());
	}

	@Override
	public T fromProperty(String property) {
		if (property == null) {
			return null;
		}
		try {
			return tlObjectMapper.get().readValue(property, type);
		} catch (IOException e) {
			throw new SpikeifyError("Error deserializing from JSON: ", e);
		}
	}

	@Override
	public String fromField(T fieldValue) {
		try {
			return tlObjectMapper.get().writeValueAsString(fieldValue);
		} catch (JsonProcessingException e) {
			throw new SpikeifyError("Error serializing to JSON: ", e);
		}
	}


}
