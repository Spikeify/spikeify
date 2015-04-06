package com.spikeify;

import com.spikeify.aerospike.*;
import com.spikeify.annotations.Ignore;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapperUtils {

	private static final int IGNORED_FIELD_MODIFIERS = Modifier.FINAL | Modifier.STATIC;

	private static List<? extends Converter> converters = Arrays.asList(
			new NoopConverter(),
			new ByteConverter(),
			new DateConverter(),
			new ShortConverter(),
			new ByteArrayConverter());

	private static Converter findConverter(Class type) {
		for (Converter converter : converters) {
			if (converter.canConvert(type)) {
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
				mappers.add(new FieldMapper(field.getName(), fieldType, fieldConverter, field));
			}

		}

		return mappers;
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
