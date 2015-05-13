package com.spikeify.entity;

import com.spikeify.annotations.*;

import java.util.*;

@SuppressWarnings("WeakerAccess")
public class EntityTx {

	@UserKey
	public Long userId;

	@SetName
	public String theSetName;

	public int one;
}
