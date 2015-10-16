package com.spikeify.generators;

import com.spikeify.UserKeyGenerator;

public class BucketKeyGenerator implements UserKeyGenerator {

	private static final String[] keys = new String[] {"A", "B", "A", "B", "C", "D", "E"}; // key 'A' and 'B' abd duplicated so retry should happen
	private static int keyPosition = 0;

	@Override
	public String generateString(int length) {

		// gets keys in order ...
		String key = keys[keyPosition];

		if (keyPosition > keys.length) {
			keyPosition = 0;
		}
		else {
			keyPosition++;
		}

		return key;
	}

	@Override
	public long generateLong(int length) {

		return 0;
	}
}
