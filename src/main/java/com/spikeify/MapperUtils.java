package com.spikeify;

import com.aerospike.client.Key;
import com.spikeify.annotations.*;
import com.spikeify.converters.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unchecked", "WeakerAccess"})
public class MapperUtils {

	private static final int IGNORED_FIELD_MODIFIERS = Modifier.FINAL | Modifier.STATIC;

	private static final List<? extends ConverterFactory> converters = Arrays.asList(
			new StringConverter(),
			new IntegerConverter(),
			new LongConverter(),
			new ByteConverter(),
			new FloatConverter(),
			new DoubleConverter(),
			new BooleanConverter(),
			new DateConverter(),
			new ShortConverter(),
			new ByteArrayConverter(),
			new SetConverterFactory(),
			new ListConverterFactory(),
			new MapConverterFactory(),
			new EnumConverterFactory()
//			,
//			new JsonConverterFactory()
	);

	public static Converter findConverter(Field field) {
		for (ConverterFactory converterFactory : converters) {
			if (converterFactory.canConvert(field.getType())) {
				return converterFactory.init(field);
			}
		}
		return null;
	}

	public static Converter findConverter(Class type) {
		for (ConverterFactory converterFactory : converters) {
			if (converterFactory.canConvert(type)) {
				return converterFactory.init(null);
			}
		}
		return null;
	}

	public static Map<String, FieldMapper> getFieldMappers(Class clazz) {

		Map<String, FieldMapper> mappers = new HashMap<>();

		for (Field field : clazz.getDeclaredFields()) {
			if (mappableField(field)) {
				Class fieldType = field.getType();
				Converter fieldConverter = findConverter(field);

				if (fieldConverter == null) {
					throw new SpikeifyError("Error: unable to map field '" + field.getDeclaringClass() + "." + field.getName() + "' " +
							"of unsupported type '" + fieldType + "'.");
				}
				mappers.put(field.getName(), new FieldMapper(getBinName(field), fieldConverter, field));
			}
		}

		return mappers;
	}


	public static Map<String, Class<? extends BigDatatypeWrapper>> getLDTClasses(Class clazz) {

		Map<String, Class<? extends BigDatatypeWrapper>> ldtMappers = new HashMap<>();

		for (Field field : clazz.getDeclaredFields()) {
			Class fieldType = field.getType();
			if (BigIndexedList.class.equals(fieldType)) {
				ldtMappers.put(field.getName(), BigIndexedList.class);
			} else if (BigMap.class.equals(fieldType)) {
				ldtMappers.put(field.getName(), BigMap.class);
			}
		}

		return ldtMappers;
	}

	public static Map<String, FieldMapper> getJsonMappers(Class clazz) {
		Map<String, FieldMapper> jsonMappers = new HashMap<>();

		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(AsJson.class) != null) {
				jsonMappers.put(field.getName(), new FieldMapper(getBinName(field), new JsonConverter(field), field));
			}
		}
		return jsonMappers;
	}

	public static String getBinName(Field field) {
		// is @BinName annotation used
		String binName = field.getName();
		if (field.getAnnotation(BinName.class) != null) {
			if (field.getAnnotation(BinName.class).value().isEmpty()) {
				throw new SpikeifyError("Error: @BinName has empty value: '" + field.getDeclaringClass() + "." + field.getName() + "'.");
			}
			binName = field.getAnnotation(BinName.class).value();
			if (binName.length() > 14) {
				throw new SpikeifyError("Error: @BinName value too long: value must be max 14 chars long, currently it's " + binName.length() +
						". Field: '" + field.getDeclaringClass() + "." + field.getName() + "'.");
			}
		}
		if (binName.length() > 14) {
			throw new SpikeifyError("Error: Field name too long: value must be max 14 chars long, currently it's " + binName.length() +
					". Field: '" + field.getDeclaringClass() + "." + field.getName() + "'.");
		}
		return binName;
	}

	public static FieldMapper<Integer, Integer> getGenerationFieldMapper(Class clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(Generation.class) != null) {
				if (int.class.equals(field.getType()) || Integer.class.equals(field.getType())) {
					return new FieldMapper(null, new PassThroughConverter(), field);
				} else {
					throw new SpikeifyError("Error: field marked with @Generation must be of type int or Integer.");
				}
			}
		}
		return null;
	}

	public static FieldMapper<Long, Long> getExpirationFieldMapper(Class clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(Expires.class) != null) {
				Class fieldType = field.getType();
				if (long.class.equals(fieldType) || Long.class.equals(fieldType)) {
					return new FieldMapper<>(null, findConverter(field), field);
				} else {
					throw new SpikeifyError("Error: field marked with @Expiration must be of type long or Long.");
				}
			}
		}
		return null;
	}

	public static FieldMapper<Map<String, ?>, Long> getAnyFieldMapper(Class clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(AnyProperty.class) != null) {
				Class fieldType = field.getType();
				Type fieldTypeParams = field.getGenericType();
				ParameterizedType paramTypes = null;
				if (fieldTypeParams instanceof ParameterizedType) {
					paramTypes = (ParameterizedType) fieldTypeParams;
				}
				if (Map.class.isAssignableFrom(fieldType) && paramTypes != null &&
						paramTypes.getActualTypeArguments()[0].equals(String.class) &&
						paramTypes.getActualTypeArguments()[1].equals(Object.class)) {
					return new FieldMapper<>(null, findConverter(field), field);
				} else {
					throw new SpikeifyError("Error: field marked with @AnyProperty must be of type long or Long.");
				}
			}
		}
		return null;
	}

	public static FieldMapper<String, String> getNamespaceFieldMapper(Class clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(Namespace.class) != null) {
				Class fieldType = field.getType();
				if (String.class.equals(fieldType)) {
					return new FieldMapper(null, findConverter(field), field);
				} else {
					throw new SpikeifyError("Error: field marked with @Namespace must be of type String.");
				}
			}
		}
		return null;
	}

	public static FieldMapper<String, String> getSetNameFieldMapper(Class clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(SetName.class) != null) {
				Class fieldType = field.getType();
				if (String.class.equals(fieldType)) {
					return new FieldMapper(null, findConverter(field), field);
				} else {
					throw new SpikeifyError("Error: field marked with @SetName must be of type String.");
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
		return !field.isAnnotationPresent(UserKey.class)
				&& !field.isAnnotationPresent(Generation.class)
				&& !field.isAnnotationPresent(Expires.class)
				&& !field.isAnnotationPresent(SetName.class)
				&& !field.isAnnotationPresent(Namespace.class)
				&& !field.isAnnotationPresent(AnyProperty.class)
				&& !field.isAnnotationPresent(AsJson.class)
				&& !field.isAnnotationPresent(Ignore.class)
				&& !BigDatatypeWrapper.class.isAssignableFrom(field.getType()) // LDT fields are not handled via normal field mappers
				&& (field.getModifiers() & IGNORED_FIELD_MODIFIERS) == 0
				&& !field.isSynthetic();
	}

	public static <TYPE> FieldMapper<Key, ?> getUserKeyFieldMapper(Class<TYPE> clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			if (field.getAnnotation(UserKey.class) != null) {
				Class fieldType = field.getType();
				if (String.class.equals(fieldType) || Long.class.equals(fieldType) || long.class.equals(fieldType)) {
					Converter converter = findConverter(field);
					return new FieldMapper(null, converter, field);
				} else {
					throw new SpikeifyError("Error: field marked with @UserKey must be of type String, Long or long.");
				}
			}
		}
		return null;
	}

}
