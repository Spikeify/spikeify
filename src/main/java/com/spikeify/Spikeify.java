package com.spikeify;

public interface Spikeify {

	<E> Loader<E> load(Class<E> type);

	<T> Updater<T> insert(T object);

	<T> Updater<T> update(T object);

	Deleter<?> delete();

//	<R> R transact(Work<R> work);
}
