package com.spikeify.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spikeify.annotations.AsJson;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntitySubJava implements Serializable {

	private String value;

	protected EntitySubJava() {

	}

	public EntitySubJava(String value) {

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


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		EntitySubJava that = (EntitySubJava) o;

		return value != null ? value.equals(that.value) : that.value == null;

	}

	@Override
	public int hashCode() {
		return value != null ? value.hashCode() : 0;
	}
}
