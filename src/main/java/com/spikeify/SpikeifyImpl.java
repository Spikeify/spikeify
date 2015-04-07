package com.spikeify;

import com.aerospike.client.AerospikeClient;
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

	private static Map<Class, ClassMapper> mappings = new ConcurrentHashMap<Class, ClassMapper>(100);

	private static ThreadLocal<Map<Object/*mapped object*/, Map<String, Object>/*record props*/>> loadedRecordsCache;

	@Override
	public  <E> Loader<E> load(Class<E> type) {
		return new Loader<>(type, synClient, asyncClient, classConstructor);
	}

	@Override
	public <T> Updater<T> insert(T object) {
		return new Updater<>(object.getClass(), object, synClient, asyncClient, classConstructor);
	}

	@Override
	public <T> Updater<T> update(T object) {
		return new Updater<>(object.getClass(), object, synClient, asyncClient, classConstructor);
	}

	public Deleter delete() {
		return null;
	}

	public <R> R transact(Work<R> work) {
		return null;
	}
}
