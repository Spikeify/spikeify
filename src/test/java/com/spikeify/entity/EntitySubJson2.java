package com.spikeify.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spikeify.annotations.AsJson;

import java.io.Serializable;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntitySubJson2 implements Serializable {

	private String value;

	protected EntitySubJson2() {

	}

	public EntitySubJson2(String value) {

		this.value = value;
	}

	@JsonProperty("value")
	public void setValue(String text) {

		value = text;
	}


	@JsonProperty("value")
	public String getValue() {

		return value;
	}
}