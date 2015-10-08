package com.spikeify.entity;

import com.spikeify.BigIndexedList;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;

public class EntityListOfBytes {

	@UserKey
	public Long userId;

	@Indexed
	public String name;

	public BigIndexedList<byte[]> data = new BigIndexedList<>();
}
