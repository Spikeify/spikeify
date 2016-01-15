package com.spikeify;

public enum ConversionTarget {

	/**
	 * The whole objects ia a target of conversion.
	 */
	DEFAULT,

	/**
	 * Convert list elements as separate objects, so that saved list will contain converted objects.
	 */
	LIST,

	/**
	 * Convert map values as separate objects, so that saved map will contain converted values.
	 */
	MAPVALUES;
}
