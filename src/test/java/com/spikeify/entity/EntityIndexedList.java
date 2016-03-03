package com.spikeify.entity;

import com.spikeify.annotations.BinName;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;

import java.util.ArrayList;
import java.util.List;

public class EntityIndexedList {

	@UserKey
	public Long userId;

	@BinName("list")
	@Indexed
	public List<Long> listLongName = new ArrayList<>();
}
