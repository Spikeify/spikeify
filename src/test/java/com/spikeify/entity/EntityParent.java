package com.spikeify.entity;

import com.spikeify.annotations.AsJson;
import com.spikeify.annotations.UserKey;

import java.util.List;
import java.util.Map;

public class EntityParent {

	@UserKey
	public Long userId;

	public Map<String, EntitySubJson> map;

	@AsJson
	public Map<String, EntitySub> jsonMap;


	public List<EntitySubJson> list;

	@AsJson
	public List<EntitySub> jsonList;
}
