package com.spikeify;

public interface Spikeify {

	<E> Loader<E> load(Class<E> type);

	<T> Updater<T> create(T object);

	<T> Updater<T> update(T object);

	<T> Deleter<T> delete();

//	<R> R transact(Work<R> work);
}
