package com.spikeify;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;

public class TypeUtils {

	/**
	 * Returns the erasure of the given type.
	 */
	public static Class<?> erase(Type type) {
		if (type instanceof Class) {
			return (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			return (Class<?>) ((ParameterizedType) type).getRawType();
		} else if (type instanceof TypeVariable) {
			TypeVariable<?> tv = (TypeVariable<?>) type;
			if (tv.getBounds().length == 0)
				return Object.class;
			else
				return erase(tv.getBounds()[0]);
		} else {
			throw new RuntimeException("not supported: " + type.getClass());
		}
	}

	public static Type getMapKeyType(Field field) {
		Type[] mapTypes = getMapTypes(field);
		return mapTypes == null ? Object.class : mapTypes[0];
	}

	public static Type getMapValueType(Field field) {
		Type[] mapTypes = getMapTypes(field);
		return mapTypes == null ? Object.class : mapTypes[1];
	}

	public static Type[] getMapTypes(Field field) {
		if (field.getType().isAssignableFrom(Map.class)) {
			if (field.getGenericType() instanceof ParameterizedType) {
				ParameterizedType mapType = (ParameterizedType) field.getGenericType();
				return mapType.getActualTypeArguments();
			}
			return null;
		} else {
			return null;
		}
	}

	public static Type getListValueType(Field field) {
		if (field.getType().isAssignableFrom(List.class)) {
			if (field.getGenericType() instanceof ParameterizedType) {
				ParameterizedType listType = (ParameterizedType) field.getGenericType();
				return listType.getActualTypeArguments()[0];
			}
			return null;
		} else {
			return Object.class;
		}
	}
}
