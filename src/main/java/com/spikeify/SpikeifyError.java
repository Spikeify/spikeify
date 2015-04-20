package com.spikeify;

public class SpikeifyError extends RuntimeException {

	public SpikeifyError(String message) {
		super(message);
	}

	public SpikeifyError(Exception originalException) {
		super(originalException);
	}

	public SpikeifyError(String message, Exception originalException) {
		super(message, originalException);
	}

}
