package com.spikeify.entity;

import com.spikeify.annotations.UserKey;

import java.util.Map;

public class EntityExists {

	@UserKey
	public Long userId;
	public int one;
	public Map map;
}
