package com.spikeify;

import com.spikeify.aerospike.*;
import com.spikeify.annotations.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapperUtils {

	private static final int IGNORED_FIELD_MODIFIERS = Modifier.FINAL | Modifier.STATIC;

	private static List<? extends Converter> converters = Arrays.asList(
			new StringConverter(),
			new IntegerConverter(),
			new FloatConverter(),
			new DoubleConverter(),
			new ByteConverter(),
			new DateConverter(),
			new ShortConverter(),
			new ByteArrayConverter());

	public static Converter findConverter(Class fieldType) {
		for (Converter converter : converters) {
			if (converter.canConvert(fieldType)) {
				return converter;
			}
		}
		return null;
	}

	public static List<FieldMapper> getFieldMappers(Class clazz) {

		List<FieldMapper> mappers = new ArrayList<FieldMapper>();

		for (Field field : clazz.getDeclaredFields()) {

			Class fieldType = field.getType();
			Converter fieldConverter = findConverter(fieldType);

			if (fieldConverter == null) {
				throw new IllegalStateException("Error: unable to map field '" + field.getDeclaringClass() + "." + field.getName() + "' " +
						"of unsupported type '" + fieldType + "'.");
			}

			if (mappableField(field)) {
				mappers.add(new FieldMapper(field.getName(), fieldConverter, field));
			}

		}

		return mappers;
	}

	public static FieldMapper getGenerationFieldMapper(Class clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(Generation.class) != null) {
				Class fieldType = field.getType();
				if (int.class.equals(field.getType()) || Integer.class.equals(field.getType())) {
					return new FieldMapper(null, findConverter(fieldType), field);
				} else {
					throw new IllegalStateException("Error: field marked with @Generation must be of type int or Integer.");
				}
			}
		}
		return null;
	}

	public static FieldMapper<Long, Long> getExpirationFieldMapper(Class clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(Expiration.class) != null) {
				Class fieldType = field.getType();
				if (long.class.equals(fieldType) || Long.class.equals(fieldType)) {
					return new FieldMapper<>(null, findConverter(fieldType), field);
				} else {
					throw new IllegalStateException("Error: field marked with @Expiration must be of type long or Long.");
				}
			}
		}
		return null;
	}

	public static FieldMapper getNamespaceFieldMapper(Class clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(Namespace.class) != null) {
				Class fieldType = field.getType();
				if (String.class.equals(fieldType)) {
					return new FieldMapper(null, findConverter(fieldType), field);
				} else {
					throw new IllegalStateException("Error: field marked with @Namespace must be of type String.");
				}
			}
		}
		return null;
	}

	public static FieldMapper getSetNameFieldMapper(Class clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(SetName.class) != null) {
				Class fieldType = field.getType();
				if (String.class.equals(fieldType)) {
					return new FieldMapper(null, findConverter(fieldType), field);
				} else {
					throw new IllegalStateException("Error: field marked with @SetName must be of type String.");
				}
			}
		}
		return null;
	}

	/**
	 * Should this field be mapped?
	 * Ignored fields with: static, final, @Ignore, synthetic modifiers
	 */
	private static boolean mappableField(Field field) {
		return !field.isAnnotationPresent(Ignore.class)
				&& (field.getModifiers() & IGNORED_FIELD_MODIFIERS) == 0
				&& !field.isSynthetic();
	}

}
