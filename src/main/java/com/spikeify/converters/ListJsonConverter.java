package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.NoArgClassConstructor;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class ListJsonConverter<T> implements Converter<List<T>, List<String>> {

	private final Constructor<? extends List> classConstructor;
	private JsonConverter<T> valueConverter;

	public ListJsonConverter(Class<? extends List> listType, Class<T> valueType) {
		this.valueConverter = new JsonConverter(valueType);

		if (listType.equals(List.class)) {
			listType = ArrayList.class;
		}
		classConstructor = NoArgClassConstructor.getNoArgConstructor(listType);
	}

	@Override
	public List<T> fromProperty(List<String> propertyList) {
		List<T> fieldList = NoArgClassConstructor.newInstance(classConstructor);
		for (String entry : propertyList) {
			fieldList.add(valueConverter.fromProperty(entry));
		}
		return fieldList;
	}

	@Override
	public List<String> fromField(List<T> fieldList) {
		List<String> propertyList = new ArrayList<>(fieldList.size());
		for (T entry : fieldList) {
			propertyList.add(valueConverter.fromField(entry));
		}
		return propertyList;
	}

}
