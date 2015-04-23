package com.spikeify;

import com.aerospike.client.Key;
import com.spikeify.commands.*;

public interface Spikeify {

	InfoFetcher info();

	<T> SingleLoader<T> get(Class<T> type);

	<T> MultiLoader<T, Key> getAll(Class<T> type, Key... keys);

	<T> MultiLoader<T, Long> getAll(Class<T> type, Long... keys);

	<T> MultiLoader<T, String> getAll(Class<T> type, String... keys);

	<T> SingleKeyUpdater<T, Key> create(Key key, T entity);

	<T> SingleKeyUpdater<T, Long> create(Long key, T entity);

	<T> SingleKeyUpdater<T, String> create(String key, T entity);

	<T> SingleObjectUpdater<T> create(T entity);

	MultiKeyUpdater createAll(Key[] keys, Object[] objects);

	MultiKeyUpdater createAll(Long[] keys, Object[] objects);

	MultiKeyUpdater createAll(String[] keys, Object[] objects);

	MultiObjectUpdater createAll(Object... entity);

	<T> SingleObjectUpdater<T> update(T object);

	<T> SingleKeyUpdater<T, Key> update(Key key, T entity);

	<T> SingleKeyUpdater<T, Long> update(Long key, T entity);

	<T> SingleKeyUpdater<T, String> update(String key, T entity);

	MultiObjectUpdater updateAll(Object... object);

	SingleObjectDeleter delete(Object object);

	SingleKeyDeleter delete(Key key);

	SingleKeyDeleter delete(Long userKey);

	SingleKeyDeleter delete(String userKey);

	MultiObjectDeleter deleteAll(Object... object);

	MultiKeyDeleter deleteAll(Key... keys);

	MultiKeyDeleter deleteAll(Long... userKeys);

	MultiKeyDeleter deleteAll(String... userKeys);

	<T> Scanner<T> query(Class<T> type);

	void truncateSet(String namespace, String setName);

	void truncateSet(Class type);

	void truncateNamespace(String namespace);


//	<R> R transact(Work<R> work);
}
