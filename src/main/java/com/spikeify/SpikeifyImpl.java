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
	public <E> SingleLoader<E> get(Class<E> type) {
		return new SingleLoader<>(type, synClient, asyncClient, classConstructor, recordsCache);
	}

	@Override
	public <E> MultiLoader<E> getAll(Class<E> type) {
		return new MultiLoader<>(type, synClient, asyncClient, classConstructor, recordsCache);
	}

	@Override
	public <T> SingleUpdater<T> create(T object) {

		if (object == null) {
			throw new IllegalStateException("Error: parameter 'object' must not be null.");
		}
		return new SingleUpdater<>(object.getClass(), synClient, asyncClient,
				recordsCache, true, object);
	}

	@Override
	public <T> MultiUpdater<T> createAll(T... objects) {

		if (objects == null || objects.length == 0) {
			throw new IllegalStateException("Error: parameter 'objects' must not be null or empty array.");
		}
		T object = objects[0];
		return new MultiUpdater<T>((Class<T>) object.getClass(), synClient, asyncClient,
				recordsCache, true, objects);
	}

	@Override
	public <T> SingleUpdater<T> update(T object) {
		if (object == null) {
			throw new IllegalStateException("Error: parameter 'object' must not be null.");
		}
		return new SingleUpdater<>(object.getClass(), synClient, asyncClient,
				recordsCache, false, object);
	}

	@Override
	public <T> MultiUpdater<T> updateAll(T... objects) {
		if (objects == null || objects.length == 0) {
			throw new IllegalStateException("Error: parameter 'objects' must not be null or empty array");
		}
		T object = objects[0];
		return new MultiUpdater<>((Class<T>) object.getClass(), synClient, asyncClient,
				recordsCache, false, objects);
	}

	@Override
	public <T> Scanner<T> query(Class<T> type) {
		return new Scanner<>(type, synClient, asyncClient, classConstructor, recordsCache);
	}

	public <T> MultiDeleter<T> delete() {
		return new MultiDeleter<>(synClient, asyncClient, recordsCache);
	}

	@Override
	public <T> MultiDeleter<T> deleteAll() {
		return new MultiDeleter<>(synClient, asyncClient, recordsCache);

	}

	public <R> R transact(Work<R> work) {
		return null;
	}
}
