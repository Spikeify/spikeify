package com.spikeify;

import com.aerospike.client.Value;

import java.lang.reflect.Type;

public interface  Converter<FIELD, PROPERTY> {

	boolean canConvert(Class type);

	public FIELD fromProperty(PROPERTY property);

	public PROPERTY fromField(FIELD fieldValue);

}
