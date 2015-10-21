package com.spikeify.entity;

import com.spikeify.annotations.UserKey;

public class EntityAutoKey {

	protected EntityAutoKey() {}

	public EntityAutoKey(String value) {
		this.value = value;
	}

	@UserKey(generate = true)
	protected String key;

	public String value;

	public String getKey() {

		return key;
	}
}
