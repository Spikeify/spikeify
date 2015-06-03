package com.spikeify.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.spikeify.Converter;
import com.spikeify.SpikeifyError;
import com.spikeify.TypeUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

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
		if (Map.class.isAssignableFrom(field.getType())) {
			Class<? extends Object> rawType = field.getType().equals(Map.class) ? HashMap.class : field.getType();
			Type keyType = TypeUtils.getMapKeyType(field);
			if (!(keyType instanceof Class)) {
				throw new SpikeifyError("Type error: @AsJson annotation can only be used on Maps where key type is a Class. " +
						"Key type used: " + keyType);
			}
			Type valueType = TypeUtils.getMapValueType(field);
			if (!(valueType instanceof Class)) {
				throw new SpikeifyError("Type error: @AsJson annotation can only be used on Maps where value type is a Class. " +
						"Value type used: " + valueType);
			}

			SimpleType keyClass = SimpleType.construct((Class<?>) keyType);
			SimpleType valueClass = SimpleType.construct((Class<?>) valueType);
			this.type = MapType.construct(rawType, keyClass, valueClass);
		} else if (Collection.class.isAssignableFrom(field.getType())) {
			Class<? extends Object> rawType = (field.getType().equals(List.class) || field.getType().equals(Collection.class))
					? ArrayList.class : field.getType();

			Type elementType = TypeUtils.getCollectionValueType(field);
			if (!(elementType instanceof Class)) {
				throw new SpikeifyError("Type error: @AsJson annotation can only be used on Collections where element type is a Class. " +
						"Element type used: " + elementType);
			}
			SimpleType elementClass = SimpleType.construct((Class<?>) elementType);

			this.type = CollectionType.construct(rawType, elementClass);
		} else {
			this.type = SimpleType.construct(field.getType());
		}
	}

	@Override
	public T fromProperty(String property) {
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
