package com.spikeify.entity;

import com.spikeify.annotations.UserKey;

public class EntityNull {

	@UserKey
	public Long userId;

	public String value;

	public Long longValue;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		EntityNull that = (EntityNull) o;

		if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;
		if (value != null ? !value.equals(that.value) : that.value != null) return false;
		return !(longValue != null ? !longValue.equals(that.longValue) : that.longValue != null);

	}

	@Override
	public int hashCode() {
		int result = userId != null ? userId.hashCode() : 0;
		result = 31 * result + (value != null ? value.hashCode() : 0);
		result = 31 * result + (longValue != null ? longValue.hashCode() : 0);
		return result;
	}
}
