package com.spikeify.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.spikeify.annotations.AsJson;

import java.util.Date;

@AsJson
@SuppressWarnings("SameParameterValue")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntitySubJson {

	public EntitySubJson() {
	}

	public EntitySubJson(int first, String second, Date date) {
		this.first = first;
		this.second = second;
		this.date = date;
	}

	public int first;
	public String second;

	@JsonIgnore
	public Date date;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		EntitySubJson that = (EntitySubJson) o;

		if (first != that.first) return false;
		if (second != null ? !second.equals(that.second) : that.second != null) return false;
		return !(date != null ? !date.equals(that.date) : that.date != null);

	}

	@Override
	public int hashCode() {
		int result = first;
		result = 31 * result + (second != null ? second.hashCode() : 0);
		result = 31 * result + (date != null ? date.hashCode() : 0);
		return result;
	}
}
