package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncClient;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpikeifyImpl<P extends Spikeify> implements Spikeify {

	public SpikeifyImpl(AerospikeClient synClient, AsyncClient asyncClient, ClassConstructor classConstructor) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
	}

	private AerospikeClient synClient;
	private AsyncClient asyncClient;
	private ClassConstructor classConstructor;

	private RecordsCache recordsCache = new RecordsCache();

	@Override
	public  <E> Loader<E> load(Class<E> type) {
		return new Loader<>(type, synClient, asyncClient, classConstructor, recordsCache);
	}

	@Override
	public <T> Updater<T> create(T object) {
		return new Updater<>(object.getClass(), object, synClient, asyncClient, classConstructor, recordsCache, true);
	}

	@Override
	public <T> Updater<T> update(T object) {
		return new Updater<>(object.getClass(), object, synClient, asyncClient, classConstructor, recordsCache, false);
	}

	public Deleter delete() {
		return new Deleter<>(synClient, asyncClient, recordsCache);
	}

	public <R> R transact(Work<R> work) {
		return null;
	}
}
