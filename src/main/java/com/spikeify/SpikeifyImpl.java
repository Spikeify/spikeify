package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.commands.*;

public class SpikeifyImpl<P extends Spikeify> implements Spikeify {

	public SpikeifyImpl(IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor, String namespace) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.namespace = namespace;
	}

	private final IAerospikeClient synClient;
	private final IAsyncClient asyncClient;
	private final ClassConstructor classConstructor;
	private final String namespace;


	private RecordsCache recordsCache = new RecordsCache();

	@Override
	public <E> SingleLoader<E> get(Class<E> type) {
		return new SingleLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, namespace);
	}

	@Override
	public <E> MultiLoader<E> getAll(Class<E> type) {
		return new MultiLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, namespace);
	}

	@Override
	public <T> SingleKeyUpdater<T> create(Key key, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, true, namespace, object, key);
	}

	@Override
	public <T> SingleKeyUpdater<T> create(Long userKey, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, true, namespace, object, userKey);
	}

	@Override
	public <T> SingleKeyUpdater<T> create(String userKey, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, true, namespace, object, userKey);
	}

	@Override
	public <T> SingleObjectUpdater<T> create(T object) {

		if (object == null) {
			throw new IllegalStateException("Error: parameter 'object' must not be null.");
		}
		return new SingleObjectUpdater<>(object.getClass(), synClient, asyncClient,
				recordsCache, true, namespace, object);
	}

	@Override
	public <T> MultiUpdater<T> createAll(T... objects) {

		if (objects == null || objects.length == 0) {
			throw new IllegalStateException("Error: parameter 'objects' must not be null or empty array.");
		}
		T object = objects[0];
		return new MultiUpdater<T>((Class<T>) object.getClass(), synClient, asyncClient,
				recordsCache, true, namespace, objects);
	}

	@Override
	public <T> SingleObjectUpdater<T> update(T object) {
		if (object == null) {
			throw new IllegalStateException("Error: parameter 'object' must not be null.");
		}
		return new SingleObjectUpdater<>(object.getClass(), synClient, asyncClient,
				recordsCache, false, namespace, object);
	}

	@Override
	public <T> SingleKeyUpdater<T> update(Key key, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, false, namespace, object, key);
	}

	@Override
	public <T> SingleKeyUpdater<T> update(Long userKey, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, false, namespace, object, userKey);
	}

	@Override
	public <T> SingleKeyUpdater<T> update(String userKey, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, false, namespace, object, userKey);
	}

	@Override
	public <T> MultiUpdater<T> updateAll(T... objects) {
		if (objects == null || objects.length == 0) {
			throw new IllegalStateException("Error: parameter 'objects' must not be null or empty array");
		}
		T object = objects[0];
		return new MultiUpdater<>((Class<T>) object.getClass(), synClient, asyncClient,
				recordsCache, false, namespace, objects);
	}

	@Override
	public <T> Scanner<T> query(Class<T> type) {
		return new Scanner<>(type, synClient, asyncClient, classConstructor, recordsCache, namespace);
	}


	public SingleObjectDeleter delete(Object object) {
		return new SingleObjectDeleter(synClient, asyncClient, recordsCache, namespace, object);
	}

	@Override
	public MultiObjectDeleter deleteAll(Object... objects) {
		return new MultiObjectDeleter(synClient, asyncClient, recordsCache, namespace, objects);
	}

	@Override
	public SingleKeyDeleter delete() {
		return new SingleKeyDeleter(synClient, asyncClient, recordsCache, namespace);
	}

	@Override
	public MultiKeyDeleter deleteAll() {
		return new MultiKeyDeleter(synClient, asyncClient, recordsCache, namespace);
	}

	public <R> R transact(Work<R> work) {
		return null;
	}

}
