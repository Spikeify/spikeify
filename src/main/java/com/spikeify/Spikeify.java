package com.spikeify;

import com.aerospike.client.Key;
import com.spikeify.commands.*;

public interface Spikeify {

	<T> SingleLoader<T> get(Class<T> type);

	<T> MultiLoader<T> getAll(Class<T> type, Key... keys);
	<T> MultiLoader<T> getAll(Class<T> type, Long... keys);
	<T> MultiLoader<T> getAll(Class<T> type, String... keys);

	<T> SingleKeyUpdater<T> create(Key key, T entity);

	<T> SingleKeyUpdater<T> create(Long key, T entity);

	<T> SingleKeyUpdater<T> create(String key, T entity);

	<T> SingleObjectUpdater<T> create(T entity);

	<T> MultiUpdater<T> createAll(T... entity);

	<T> SingleObjectUpdater<T> update(T object);

	<T> SingleKeyUpdater<T> update(Key key, T entity);

	<T> SingleKeyUpdater<T> update(Long key, T entity);

	<T> SingleKeyUpdater<T> update(String key, T entity);

	<T> MultiUpdater<T> updateAll(T... object);

	SingleObjectDeleter delete(Object object);

	SingleKeyDeleter delete(Key key);

	SingleKeyDeleter delete(Long userKey);

	SingleKeyDeleter delete(String userKey);

	MultiObjectDeleter deleteAll(Object... object);

	MultiKeyDeleter deleteAll(Key... keys);

	MultiKeyDeleter deleteAll(Long... userKeys);

	MultiKeyDeleter deleteAll(String... userKeys);

	<T> Scanner<T> query(Class<T> type);

//	<R> R transact(Work<R> work);
}
