package com.spikeify.entity;

import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeMap;
import com.spikeify.BigIndexedList;
import com.spikeify.annotations.UserKey;

public class EntityLDT {

	@UserKey
	public Long userId;

	public int one;

	public BigIndexedList<Long> list;
	public BigIndexedList<EntitySubJson> jsonList;

}
