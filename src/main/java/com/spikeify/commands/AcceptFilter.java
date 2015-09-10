package com.spikeify.commands;

public interface AcceptFilter<T> {

	/**
	 * @param item to be examined
	 * @return true if item has expected properties, false otherwise
	 */
	boolean accept(T item);
}
