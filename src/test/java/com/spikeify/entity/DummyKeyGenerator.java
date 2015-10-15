package com.spikeify.entity;

import com.spikeify.UserKeyGenerator;

/**
 * Creates always the same key ... forcing create to fail
 */
public class DummyKeyGenerator implements UserKeyGenerator {

	@Override
	public String generateString(int length) {

		return "A";
	}

	@Override
	public long generateLong(int length) {

		return 1;
	}
}
