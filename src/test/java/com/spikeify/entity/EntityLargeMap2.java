package com.spikeify.entity;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.spikeify.BigMap;
import com.spikeify.annotations.AsJson;
import com.spikeify.annotations.UserKey;

/**
 *
 */
public class EntityLargeMap2 {

	@UserKey(generate = true)
	public String userId;

	public String bla;

	public BigMap<Long, EntitySubJson2> jsonMap;
}
