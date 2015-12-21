package com.spikeify.generators;

import java.math.BigInteger;
import java.security.SecureRandom;

/**
 * Will generate and random Id (BigInt)
 */
public final class IdGenerator {

	static final String ELEMENTS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvqxyz";
	public static final int KEY_LENGHT = 10;

	private IdGenerator() {
		// hide constructor
	}

	/**
	 * Generates a random big integer number
	 *
	 * @return big integer random
	 */
	public static BigInteger generate() {

		BigInteger start = BigInteger.valueOf(System.currentTimeMillis());
		SecureRandom random = new SecureRandom();
		int nlen = start.bitLength();

		BigInteger temp = start.subtract(BigInteger.ONE);
		BigInteger result, seed;
		do {
			seed = new BigInteger(nlen + 100, random);
			result = seed.mod(start);
		}
		while (seed.subtract(result).add(temp).bitLength() >= nlen + 100);

		return result;
	}

	/**
	 * Generates a random string with characters from ELEMENTS of a desired length
	 * @param length desired lenght  1..100
	 * @return random string
	 */
	public static String generate(int length) {

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

	public static String generateKey() {
		return generate(KEY_LENGHT);
	}
}
