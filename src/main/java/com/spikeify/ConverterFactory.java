package com.spikeify;

public interface ConverterFactory {

	Converter init(Class type);

	boolean canConvert(Class type);

}
