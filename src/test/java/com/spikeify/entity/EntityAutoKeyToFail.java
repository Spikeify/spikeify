package com.spikeify.entity;

import com.spikeify.annotations.UserKey;

public class EntityAutoKeyToFail {

	@UserKey(generate = true, keyLength = 10, generator = DummyKeyGenerator.class)
	public long key;

	public String value;
}
