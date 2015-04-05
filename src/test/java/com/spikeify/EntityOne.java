package com.spikeify;

import com.spikeify.annotations.Ignore;
import com.spikeify.annotations.Record;

@Record
public class EntityOne {

	public int one;
	public String two;
	public double three;
	public float four;
	private short five;
	private byte six;
	public byte[] seven;

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
}
