package com.spikeify;

public class ExpirationUtils {

	public static int AS_TIMEBASE_SEC = 1262304000;  // Unix timestamp in seconds of Jan 01 2010 00:00:00 GMT
	public static long AS_TIMEBASE_MILIS = 1000L * AS_TIMEBASE_SEC;  // Unix timestamp in milliseconds of Jan 01 2010 00:00:00 GMT

	/**
	 * @param recordExpirationTimestamp Timestamp when record expires, in seconds since Jan 01 2010 00:00:00 GMT (absolute)
	 * @return Expiration time in millis
	 */
	public static long getExpirationMillisAbs(int recordExpirationTimestamp) {
		long timestamp;

		// Aerospike expiry settings are messed up: you put in -1 and get back 0
		if (recordExpirationTimestamp == 0) {
			timestamp = -1; // default expiration setting: -1 - no expiration set
		} else {
			// convert record expiration time (seconds from 01/01/2010 0:0:0 GMT)
			// to java epoch time in milliseconds
			timestamp = 1000L * (AS_TIMEBASE_SEC + recordExpirationTimestamp);
		}
		return timestamp;
	}

	/**
	 * @param recordExpirationTimestamp Timestamp when record expires, in seconds since Jan 01 2010 00:00:00 GMT (relative)
	 * @return Expiration time in seconds
	 */
	public static long getExpirationMillisRelative(long recordExpirationTimestamp) {
		long timestamp;

		// Aerospike expiry settings are messed up: you put in -1 and get back 0
		if (recordExpirationTimestamp == 0) {
			timestamp = -1; // default expiration setting: -1 - no expiration set
		} else {
			// convert record expiration time (seconds from 01/01/2010 0:0:0 GMT)
			// to java epoch time in milliseconds
			long now = System.currentTimeMillis() / 1000;
			timestamp = (AS_TIMEBASE_SEC + recordExpirationTimestamp) - now;
		}
		return timestamp;
	}

	/**
	 * Returns relative expiration time of a record in seconds from now.
	 *
	 * @param expiresTimestampMillis A Unix timestamp in milliseconds when record should expire
	 * @return Expiration time of a record in seconds from now
	 */
	public static int getRecordExpiration(long expiresTimestampMillis) {
		int recordExpiration;

		if (expiresTimestampMillis == 0 || expiresTimestampMillis == -1) {
			recordExpiration = (int) expiresTimestampMillis; // default expiration settings: 0 - server default expiration, -1 - no expiration
		} else {
			// convert absolute java timestamp in milliseconds to a relative expiration time in seconds from now
			long now = System.currentTimeMillis();
			recordExpiration = (int) ((expiresTimestampMillis - now) / 1000L);
		}
		return recordExpiration;
	}

	/**
	 * Transforms an absolute record expiration time in seconds since Jan 01 2010 00:00:00 GMT to relative expiration time in seconds from now.
	 * @param absRecordExpiresTimestamp absolute time stamp
	 * @return Relative expiration time in seconds from now
	 */
	public static int getExpiresRelative(int absRecordExpiresTimestamp) {

		if (absRecordExpiresTimestamp == 0 || absRecordExpiresTimestamp == -1) {
			return absRecordExpiresTimestamp;
		} else {
			return absRecordExpiresTimestamp + AS_TIMEBASE_SEC - nowSeconds();
		}
	}

	private static long nowMillis(){
		return System.currentTimeMillis();
	}

	private static int nowSeconds(){
		return (int) (System.currentTimeMillis() / 1000);
	}

}
