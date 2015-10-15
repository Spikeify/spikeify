package com.spikeify;

import java.security.SecureRandom;

/**
 * Default implementation of UserKeyGenerator used in case no other is provided
 */
public class DefaultKeyGenerator implements UserKeyGenerator {

	static final String NUMBERS_WITHOUT_ZERO = "123456789";
	static final String NUMBERS  = "0" + NUMBERS_WITHOUT_ZERO;
	static final String ELEMENTS = NUMBERS + "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvqxyz";

	@Override
	public String generateString(int length) {

		if (length <= 0 || length > 100) {
			throw new IllegalArgumentException("Can't generate random id with length: " + length);
		}

		SecureRandom random = new SecureRandom();
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			sb.append(ELEMENTS.charAt(random.nextInt(ELEMENTS.length())));
		}

		return sb.toString();
	}

	@Override
	public long generateLong(int length) {

		if (length <= 0 || length > 100) {
			throw new IllegalArgumentException("Can't generate random id with length: " + length);
		}

		SecureRandom random = new SecureRandom();
		StringBuilder sb = new StringBuilder(length);

		// 1st digit should  not be a 0
		sb.append(NUMBERS_WITHOUT_ZERO.charAt(random.nextInt(NUMBERS_WITHOUT_ZERO.length())));

		// all other digits can contain a 0
		for (int i = 1; i < length; i++) {
			sb.append(NUMBERS.charAt(random.nextInt(NUMBERS.length())));
		}

		return Long.parseLong(sb.toString());
	}

}
