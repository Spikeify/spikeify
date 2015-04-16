package com.spikeify;

public interface Spikeify {

	<T> SingleLoader<T> get(Class<T> type);

	<T> MultiLoader<T> getAll(Class<T> type);

	<T> SingleUpdater<T> create(T object);

	<T> MultiUpdater<T> createAll(T... object);

	<T> SingleUpdater<T> update(T object);

	<T> MultiUpdater<T> updateAll(T... object);

	<T> Deleter<T> delete();

	<T> Scanner<T> query(Class<T> type);

//	<R> R transact(Work<R> work);
}
