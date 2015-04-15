package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.IAsyncClient;

public class SpikeifyImpl<P extends Spikeify> implements Spikeify {

	public SpikeifyImpl(IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
	}

	private IAerospikeClient synClient;
	private IAsyncClient asyncClient;
	private ClassConstructor classConstructor;

	private RecordsCache recordsCache = new RecordsCache();

	@Override
	public <E> Loader<E> load(Class<E> type) {
		return new Loader<>(type, synClient, asyncClient, classConstructor, recordsCache);
	}

	@Override
	public <T> Updater<T> create(T... objects) {

		if (objects == null || objects.length == 0) {
			throw new IllegalStateException("Error: parameter 'objects' must not be null or empty array");
		}
		T object = objects[0];
		return new Updater<>(object.getClass(), synClient, asyncClient,
				recordsCache, true, objects);
	}

	@Override
	public <T> Updater<T> update(T... objects) {
		if (objects == null || objects.length == 0) {
			throw new IllegalStateException("Error: parameter 'objects' must not be null or empty array");
		}
		T object = objects[0];
		return new Updater<>(object.getClass(), synClient, asyncClient,
				recordsCache, false, objects);
	}

	public Deleter delete() {
		return new Deleter<>(synClient, asyncClient, recordsCache);
	}

	public <R> R transact(Work<R> work) {
		return null;
	}
}
