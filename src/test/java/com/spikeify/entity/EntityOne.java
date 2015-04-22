package com.spikeify.entity;

import com.spikeify.annotations.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Record
public class EntityOne {

	@UserKey
	public Long userId;

	public int one;
	public String two;
	public double three;
	public float four;
	private short five;
	private byte six;
	public boolean seven;
	public Date eight;
	public List nine;
	public Map ten;
	public EntityEnum eleven;

	@AnyProperty
	public Map<String, Object> unmapped = new HashMap<>();

	@SetName
	public String theSetName;

	// unmappable class must be ignored
	@Ignore
	public java.util.Calendar calendar;

	@Ignore
	public String ignored;

	public short getFive() {
		return five;
	}

	public void setFive(short five) {
		this.five = five;
	}

	public byte getSix() {
		return six;
	}

	public void setSix(byte six) {
		this.six = six;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		EntityOne entityOne = (EntityOne) o;

		if (one != entityOne.one) return false;
		if (Double.compare(entityOne.three, three) != 0) return false;
		if (Float.compare(entityOne.four, four) != 0) return false;
		if (five != entityOne.five) return false;
		if (six != entityOne.six) return false;
		if (seven != entityOne.seven) return false;
		if (two != null ? !two.equals(entityOne.two) : entityOne.two != null) return false;
		return !(ignored != null ? !ignored.equals(entityOne.ignored) : entityOne.ignored != null);
	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		result = one;
		result = 31 * result + (two != null ? two.hashCode() : 0);
		temp = Double.doubleToLongBits(three);
		result = 31 * result + (int) (temp ^ (temp >>> 32));
		result = 31 * result + (four != +0.0f ? Float.floatToIntBits(four) : 0);
		result = 31 * result + (int) five;
		result = 31 * result + (int) six;
		result = 31 * result + (seven ? 1 : 0);
		result = 31 * result + (ignored != null ? ignored.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "EntityOne{" +
				"userId=" + userId +
				", one=" + one +
				", two='" + two + '\'' +
				", three=" + three +
				", four=" + four +
				", five=" + five +
				", six=" + six +
				", seven=" + seven +
				", eight=" + eight +
				", nine=" + nine +
				", ten=" + ten +
				", theSetName='" + theSetName + '\'' +
				", calendar=" + calendar +
				", ignored='" + ignored + '\'' +
				'}';
	}
}
