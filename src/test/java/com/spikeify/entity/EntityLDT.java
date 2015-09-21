package com.spikeify.entity;

import com.spikeify.BigIndexedList;
import com.spikeify.annotations.UserKey;

public class EntityLDT {

	@UserKey
	public Long userId;

	public int one;

	public BigIndexedList<Long> list;
	public BigIndexedList<EntitySubJson> jsonList;

}
