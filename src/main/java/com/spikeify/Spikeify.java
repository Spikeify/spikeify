package com.spikeify;

public interface Spikeify {

	<E> Loader<E> load(Class<E> type);

	Updater<?> insert();

	Updater<?> update();

	Deleter<?> delete();

//	<R> R transact(Work<R> work);
}
