package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.AsyncClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpikeifyImpl implements Spikeify {

	public SpikeifyImpl(AerospikeClient synClient, AsyncClient asyncClient, ClassConstructor classConstructor) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
	}

	private AerospikeClient synClient;
	private AsyncClient asyncClient;
	private ClassConstructor classConstructor;

	private static Map<Class, ClassMapper> mappings = new ConcurrentHashMap<Class, ClassMapper>(100);

	private static ThreadLocal<Map<Object/*mapped object*/, Map<String, Object>/*record props*/>> loadedRecordsCache;

	public Loader load() {
		return new Loader(synClient, asyncClient, classConstructor);
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
