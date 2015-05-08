package com.spikeify.entity;

import com.spikeify.annotations.*;

import java.util.*;

public class EntityOne {

	@UserKey
	public Long userId;

	@SetName
	public String theSetName;

	@Generation
	public Integer generation;

	public int one;
	public String two;

	@BinName("third")  // explicitly set the name of the bin	public double three;
	public double three;
	public float four;
	private short five;
	private byte six;
	public boolean seven;
	public Date eight;
	public List<String> nine;
	public Map ten;
	public EntityEnum eleven;
	public Set twelve;

	@AsJson
	public EntitySub sub;

	@AnyProperty
	public Map<String, Object> unmapped = new HashMap<>();

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

	// below: generated equals() & hashCode() methods for easier object comparing
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
		if (userId != null ? !userId.equals(entityOne.userId) : entityOne.userId != null) return false;
		if (two != null ? !two.equals(entityOne.two) : entityOne.two != null) return false;
		if (eight != null ? !eight.equals(entityOne.eight) : entityOne.eight != null) return false;
		if (nine != null ? !nine.equals(entityOne.nine) : entityOne.nine != null) return false;
		if (ten != null ? !ten.equals(entityOne.ten) : entityOne.ten != null) return false;
		if (eleven != entityOne.eleven) return false;
		if (twelve != null ? !twelve.equals(entityOne.twelve) : entityOne.twelve != null) return false;
		if (sub != null ? !sub.equals(entityOne.sub) : entityOne.sub != null) return false;
		if (unmapped != null ? !unmapped.equals(entityOne.unmapped) : entityOne.unmapped != null) return false;
		if (theSetName != null ? !theSetName.equals(entityOne.theSetName) : entityOne.theSetName != null) return false;
		if (calendar != null ? !calendar.equals(entityOne.calendar) : entityOne.calendar != null) return false;
		return !(ignored != null ? !ignored.equals(entityOne.ignored) : entityOne.ignored != null);

	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		result = userId != null ? userId.hashCode() : 0;
		result = 31 * result + one;
		result = 31 * result + (two != null ? two.hashCode() : 0);
		temp = Double.doubleToLongBits(three);
		result = 31 * result + (int) (temp ^ (temp >>> 32));
		result = 31 * result + (four != +0.0f ? Float.floatToIntBits(four) : 0);
		result = 31 * result + (int) five;
		result = 31 * result + (int) six;
		result = 31 * result + (seven ? 1 : 0);
		result = 31 * result + (eight != null ? eight.hashCode() : 0);
		result = 31 * result + (nine != null ? nine.hashCode() : 0);
		result = 31 * result + (ten != null ? ten.hashCode() : 0);
		result = 31 * result + (eleven != null ? eleven.hashCode() : 0);
		result = 31 * result + (twelve != null ? twelve.hashCode() : 0);
		result = 31 * result + (sub != null ? sub.hashCode() : 0);
		result = 31 * result + (unmapped != null ? unmapped.hashCode() : 0);
		result = 31 * result + (theSetName != null ? theSetName.hashCode() : 0);
		result = 31 * result + (calendar != null ? calendar.hashCode() : 0);
		result = 31 * result + (ignored != null ? ignored.hashCode() : 0);
		return result;
	}
}
