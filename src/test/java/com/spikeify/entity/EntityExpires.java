package com.spikeify.entity;

import com.spikeify.annotations.Expires;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.SetName;
import com.spikeify.annotations.UserKey;

@SuppressWarnings("WeakerAccess")
public class EntityExpires {

	@UserKey
	public Long userId;

	@Generation
	public int generation;

	@SetName
	public String theSetName;

	@Expires
	public Long expires;

	public int one;
}
