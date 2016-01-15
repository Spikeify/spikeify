package com.spikeify.entity;

import com.spikeify.BigMap;
import com.spikeify.ConversionTarget;
import com.spikeify.annotations.AsJson;
import com.spikeify.annotations.UserKey;

/**
 *
 */
public class EntityLargeMap3 {

	@UserKey(generate = true)
	public String userId;

	public String bla;

	@AsJson(target = ConversionTarget.MAPVALUES)
	private BigMap<Long, EntitySubJava> jsonMap;

	public void put(long key, EntitySubJava value) {

		if (jsonMap == null) {
			jsonMap = new BigMap<>();
		}

		jsonMap.put(key, value);
	}

	public BigMap<Long, EntitySubJava> getMap(){
		return jsonMap;
	}
}
