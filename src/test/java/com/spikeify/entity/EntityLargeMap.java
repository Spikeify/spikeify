package com.spikeify.entity;

import com.spikeify.BigMap;
import com.spikeify.annotations.UserKey;

public class EntityLargeMap {

	@UserKey
	public Long userId;

	public int one;

	public BigMap<Long, Long> map;
	public BigMap<String, Long> stringMap;
	public BigMap<Long, EntitySubJson> jsonMap;

}
