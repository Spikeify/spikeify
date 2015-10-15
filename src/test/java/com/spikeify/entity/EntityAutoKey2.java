package com.spikeify.entity;

import com.spikeify.annotations.UserKey;

public class EntityAutoKey2 {

	@UserKey(generate = true, keyLength = 2)
	public Long key;

	public String value;
}
