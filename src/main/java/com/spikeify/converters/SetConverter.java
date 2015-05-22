package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.NoArgClassConstructor;
import com.spikeify.SpikeifyError;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings("unchecked")
public class SetConverter<T> implements Converter<Set<T>, List<T>> {

	private final Class<? extends Set> setType;
	private final Constructor<? extends Set> noArgConstructor;

	public SetConverter(Class<? extends Set> setType) {
		if (setType.equals(Set.class)) {
			setType = HashSet.class;
		}

		this.setType = setType;
		this.noArgConstructor = NoArgClassConstructor.getNoArgConstructor(setType);
	}

	public boolean canConvert(Class type) {
		return List.class.isAssignableFrom(type);
	}

	public Set<T> fromProperty(List<T> list) {
		Set set;
		try {
			set = noArgConstructor.newInstance();
		} catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
			throw new SpikeifyError("Error: could not instantiate '" + setType + "' via a default no-arg constructor.");
		}
		set.addAll(list);
		return set;
	}

	public List<T> fromField(Set<T> fieldValue) {
		return new ArrayList(fieldValue);
	}

}
