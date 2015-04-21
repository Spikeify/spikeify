package com.spikeify;

import com.aerospike.client.Key;

public enum KeyType {

	KEY (Key.class),
	LONG (Long.class),
	STRING (String.class);

	private Class keyType;

	KeyType(Class type){
		this.keyType = type;
	}
}
