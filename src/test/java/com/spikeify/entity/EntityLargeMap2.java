package com.spikeify.entity;

import com.spikeify.BigMap;
import com.spikeify.annotations.UserKey;

public class EntityLargeMap2 {

	@UserKey(generate = true)
	public String userId;

	public String bla;

	// no AsJson was used - EntitySubJava will be serialized via Java serialization
	public BigMap<Long, EntitySubJava> javaMap;
}
