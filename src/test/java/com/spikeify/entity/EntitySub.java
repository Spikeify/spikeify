package com.spikeify.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.spikeify.annotations.AsJson;

import java.util.Date;

@SuppressWarnings("SameParameterValue")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntitySub {

	public EntitySub() {
	}

	public EntitySub(int first, String second, Date date) {
		this.first = first;
		this.second = second;
		this.date = date;
	}

	public int first;
	public String second;

	@JsonIgnore
	public Date date;

}
