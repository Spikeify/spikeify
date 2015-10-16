package com.spikeify.entity;

import com.spikeify.annotations.UserKey;

public class EntityAutoKey {

	protected EntityAutoKey() {}

	public EntityAutoKey(String value) {
		this.value = value;
	}

	@UserKey(generate = true)
	public String key;

	public String value;
}
