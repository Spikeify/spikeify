package com.spikeify;

/**
 * Id generator to produce entity keys on create
 */
public interface UserKeyGenerator {

	/**
	 * Generates string key of given length greater than 0
	 * @param length desired length of key greater than 0
	 * @return generated key
	 */
	String generateString(int length);

	/**
	 * Generates long value of given length greater than 0
	 * @param length desired length of key greater than 0 (must consider max possible length of a long value)
	 * @return generated key
	 */
	long generateLong(int length);
}
