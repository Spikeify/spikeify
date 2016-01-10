package com.spikeify.entity;

import com.spikeify.BigIndexedList;
import com.spikeify.BigMap;
import com.spikeify.annotations.UserKey;

public class EntityLargeMap {

	@UserKey
	public Long userId;

	public int one;

	public BigMap<Long, Long> map;
	public BigMap<Long, EntitySubJson> jsonMap;

}
