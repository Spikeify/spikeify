package com.spikeify;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A ClassConstructor implementation that uses no-argument constructor to instantiate objects
 */
@SuppressWarnings("WeakerAccess")
public class NoArgClassConstructor implements ClassConstructor {

	public <T> T construct(Class<T> type) {
		Constructor<T> ctor = NoArgClassConstructor.getNoArgConstructor(type);
		return NoArgClassConstructor.newInstance(ctor);
	}

	public static <T> T newInstance(Constructor<T> ctor, Object... params) {
		try {
			return ctor.newInstance(params);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Throw an SpikeifyError if the class does not have a no-arg constructor.
	 */
	public static <T> Constructor<T> getNoArgConstructor(Class<T> clazz) {
		try {
			Constructor<T> ctor = clazz.getDeclaredConstructor();
			ctor.setAccessible(true);
			return ctor;
		} catch (NoSuchMethodException e) {
			if (clazz.isMemberClass() || clazz.isAnonymousClass() || clazz.isLocalClass())
				throw new SpikeifyError(clazz.getName() + " must be static and must have a no-arg constructor", e);
			else
				throw new SpikeifyError(clazz.getName() + " must have a no-arg constructor", e);
		}
	}
}
