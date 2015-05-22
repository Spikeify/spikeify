package com.spikeify;

import java.lang.reflect.Field;

public interface ConverterFactory {

	Converter init(Field field);

	boolean canConvert(Class type);

}
