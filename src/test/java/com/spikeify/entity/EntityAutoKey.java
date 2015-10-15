package com.spikeify.entity;

import com.spikeify.annotations.UserKey;

public class EntityAutoKey {

	@UserKey(generate = true)
	public String key;

	public String value;
}
