package com.spikeify.entity;

import com.spikeify.generators.DummyKeyGenerator;
import com.spikeify.annotations.UserKey;

public class EntityAutoKeyToFail {

	protected EntityAutoKeyToFail() {}

	public EntityAutoKeyToFail(String value) {
		this.value = value;
	}

	@UserKey(generate = true, keyLength = 10, generator = DummyKeyGenerator.class)
	public long key;

	public String value;
}
