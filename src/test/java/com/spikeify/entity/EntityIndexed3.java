package com.spikeify.entity;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.SetName;
import com.spikeify.annotations.UserKey;

/**
 * For testing indexing service
 */
@SetName("EntityIndexed") // change ... so some entity name os other entity
public class EntityIndexed3 {

	@UserKey
	@Indexed // should be ignored
	public String key;

	@Generation
	public Integer generation;

	@Indexed(name = "failed_index")
	public String text;
}
