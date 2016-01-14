package com.spikeify.entity;

import com.spikeify.BigMap;
import com.spikeify.annotations.AsJson;
import com.spikeify.annotations.UserKey;

/**
 *
 */
public class EntityLargeMap3 {

	@UserKey(generate = true)
	public String userId;

	public String bla;

	// we should be able to hide the property
//	@AsJson
	private BigMap<Long, EntitySubJson2> jsonMap;

	public void put(long key, EntitySubJson2 value) {

		if (jsonMap == null) {
			jsonMap = new BigMap<>();
		}

		jsonMap.put(key, value);
	}
}
