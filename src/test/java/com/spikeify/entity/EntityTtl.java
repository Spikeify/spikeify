package com.spikeify.entity;

import com.spikeify.annotations.*;

@SuppressWarnings("WeakerAccess")
public class EntityTtl {

	@UserKey
	public Long userId;

	@Generation
	public int generation;

	@SetName
	public String theSetName;

	@TimeToLive
	public Long ttl;

	public int one;
}
