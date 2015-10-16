package com.spikeify.entity;

import com.spikeify.annotations.UserKey;

public class EntityAutoKey2 {

	protected EntityAutoKey2() {}

	public EntityAutoKey2(String value) {
		this.value = value;
	}

	@UserKey(generate = true, keyLength = 2)
	public Long key;

	public String value;
}
