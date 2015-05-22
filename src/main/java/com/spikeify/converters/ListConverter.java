package com.spikeify.converters;

import com.spikeify.Converter;
import com.spikeify.ConverterFactory;

import java.util.List;

public class ListConverter implements Converter<List, List>{

	public List fromProperty(List list) {
		return list;
	}

	public List fromField(List fieldValue) {
		return fieldValue;
	}

}
