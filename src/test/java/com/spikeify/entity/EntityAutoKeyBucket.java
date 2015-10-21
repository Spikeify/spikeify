package com.spikeify.entity;

import com.spikeify.annotations.UserKey;
import com.spikeify.generators.BucketKeyGenerator;

public class EntityAutoKeyBucket {

	protected EntityAutoKeyBucket() {}

	public EntityAutoKeyBucket(String value) {
		this.value = value;
	}

	@UserKey(generate = true, generator = BucketKeyGenerator.class)
	protected String key;

	public String value;

	public String getKey() {

		return key;
	}
}
