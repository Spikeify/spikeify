package com.spikeify;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpikeifyImpl implements Spikeify {

	private static Map<Class, ClassMapper> mappings = new ConcurrentHashMap<Class, ClassMapper>(100);

	public Loader load() {
		return null;
	}

	public Updater insert() {
		return null;
	}

	public Updater update() {
		return null;
	}

	public Deleter delete() {
		return null;
	}

	public <R> R transact(Work<R> work) {
		return null;
	}
}
