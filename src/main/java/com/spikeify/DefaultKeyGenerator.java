package com.spikeify;

import java.security.SecureRandom;

/**
 * Default implementation of UserKeyGenerator used in case no other is provided
 */
public class DefaultKeyGenerator implements UserKeyGenerator {

	static final String NUMBERS_NO_ZERO = "123456789";
	static final String NUMBERS_NO_NINE_AND_ZERO = "12345678";

	static final String NUMBERS  = "0" + NUMBERS_NO_ZERO;
	static final String ELEMENTS = NUMBERS + "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	/**
	 * Generates random string from ELEMENTS set of chars and numbers
	 * @param length desired length of key > 0
	 * @return random string of desired length (min 1, max 100 characters long)
	 */
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

	/**
	 * Generates non negative long key of maximum length 18
	 * @param length desired length of key > 0 (must consider max possible length of a long value)
	 * @return long key (min 1, max 19 digits long)
	 */
	@Override
	public long generateLong(int length) {

		if (length <= 0 || length > 19) {
			throw new IllegalArgumentException("Can't generate random id with length: " + length);
		}

		SecureRandom random = new SecureRandom();
		StringBuilder sb = new StringBuilder(length);

		// 1st digit should  not be a 0 or 9 if desired length is 19 (as max long is: 9223372036854775807)

		if (length == 19) {
			sb.append(NUMBERS_NO_NINE_AND_ZERO.charAt(random.nextInt(NUMBERS_NO_NINE_AND_ZERO.length())));
		}
		else {
			sb.append(NUMBERS_NO_ZERO.charAt(random.nextInt(NUMBERS_NO_ZERO.length())));
		}

		// all other digits can contain a 0
		for (int i = 1; i < length; i++) {
			sb.append(NUMBERS.charAt(random.nextInt(NUMBERS.length())));
		}

		return Long.parseLong(sb.toString());
	}
}
