package com.spikeify;

import java.lang.reflect.Type;

public interface ConverterFactory {

	Converter init(Type type);

	boolean canConvert(Class type);

}
