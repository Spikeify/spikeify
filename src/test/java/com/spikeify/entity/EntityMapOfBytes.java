package com.spikeify.entity;

import com.spikeify.BigIndexedList;
import com.spikeify.BigMap;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;

public class EntityMapOfBytes {

	@UserKey
	public Long userId;

	@Indexed
	public String name;

	public BigMap<Long, byte[]> data = new BigMap<>();
}
