package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.NoArgClassConstructor;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class ListJsonConverter implements Converter<List, List> {

	private final Constructor<? extends List> classConstructor;
	private JsonConverter valueConverter;

	@SuppressWarnings("unchecked")
	public ListJsonConverter(Class<? extends List> listType, Class valueType) {
		this.valueConverter = new JsonConverter(valueType);

		if (listType.equals(List.class)) {
			listType = ArrayList.class;
		}
		classConstructor = NoArgClassConstructor.getNoArgConstructor(listType);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List fromProperty(List propertyList) {
		List fieldList = NoArgClassConstructor.newInstance(classConstructor);
		for (Object entry : propertyList) {
			if(entry instanceof String){
				fieldList.add(valueConverter.fromProperty((String) entry));
			} else {
				fieldList.add(entry);
			}
		}
		return fieldList;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<String> fromField(List fieldList) {
		List<String> propertyList = new ArrayList<>(fieldList.size());
		for (Object entry : fieldList) {
			propertyList.add(valueConverter.fromField(entry));
		}
		return propertyList;
	}

}
