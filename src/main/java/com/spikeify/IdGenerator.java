package com.spikeify;

import com.spikeify.annotations.UserKey;

import java.lang.reflect.Field;

/**
 * Will generate and random Id (BigInt)
 */
public final class IdGenerator {

	static final String NUMBERS_WITHOUT_ZERO = "123456789";
	static final String NUMBERS  = "0" + NUMBERS_WITHOUT_ZERO;
	static final String ELEMENTS = NUMBERS + "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvqxyz";

	private IdGenerator() {
		// hide constructor
	}

	/**
	 * Generates id of given object if UserKey annotation with generate() flag is set
	 * uses UserKeyGenerator class to produce key
	 * @param object to set UserKey
	 */
	public static void generateId(Object object) {

		for (Field field : object.getClass().getDeclaredFields()) {

			try {
				UserKey annotation = field.getAnnotation(UserKey.class);
				if (annotation != null && annotation.generate()) {

					UserKeyGenerator generator = annotation.generator().newInstance();

					if (String.class.isAssignableFrom(field.getType())) {
						String id = generator.generateString(annotation.keyLength());
						field.setAccessible(true);
						field.set(object, id);
					}

					if (long.class.isAssignableFrom(field.getType())) {
						long id = generator.generateLong(annotation.keyLength());
						field.setAccessible(true);
						field.setLong(object, id);
					}

					if (Long.class.isAssignableFrom(field.getType())) {
						Long id = generator.generateLong(annotation.keyLength());
						field.setAccessible(true);
						field.set(object, id);
					}

					break;
				}
			}
			catch (IllegalAccessException | InstantiationException e) {
				throw new SpikeifyError("Failed to generate id for: " + object, e);
			}
		}
	}

	public static <T> boolean shouldGenerateId(Object object) {

		for (Field field : object.getClass().getDeclaredFields()) {

			UserKey annotation = field.getAnnotation(UserKey.class);
			if (annotation != null) {
				return annotation.generate();
			}
		}

		return false;
	}
}
