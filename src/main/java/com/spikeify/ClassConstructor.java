package com.spikeify;

/**
 * An interface to represent class constructors.
 * You can easily create your own constructor that uses alternative methods for instantiating classes, like DI, etc..
 */
public interface ClassConstructor {

	<T> T construct(Class<T> type);
}
