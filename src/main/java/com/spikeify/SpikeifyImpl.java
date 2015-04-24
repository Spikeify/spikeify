package com.spikeify;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.lua.LuaAerospikeLib;
import com.spikeify.commands.*;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SpikeifyImpl<P extends Spikeify> implements Spikeify {

	private static Logger log = Logger.getLogger(SpikeifyImpl.class.getSimpleName());

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
	public InfoFetcher info() {
		return new InfoFetcher(synClient);
	}

	@Override
	public <E> SingleLoader<E> get(Class<E> type) {
		return new SingleLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, namespace);
	}

	@Override
	public <E> MultiLoader<E, Key> getAll(Class<E> type, Key... keys) {
		return new MultiLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, namespace, keys);
	}

	@Override
	public <E> MultiLoader<E, Long> getAll(Class<E> type, Long... keys) {
		return new MultiLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, namespace, keys);
	}

	@Override
	public <E> MultiLoader<E, String> getAll(Class<E> type, String... keys) {
		return new MultiLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, namespace, keys);
	}

	@Override
	public <T> SingleKeyUpdater<T, Key> create(Key key, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, true, namespace, object, key);
	}

	@Override
	public <T> SingleKeyUpdater<T, Long> create(Long userKey, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, true, namespace, object, userKey);
	}

	@Override
	public <T> SingleKeyUpdater<T, String> create(String userKey, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, true, namespace, object, userKey);
	}

	@Override
	public <T> SingleObjectUpdater<T> create(T object) {

		if (object == null) {
			throw new SpikeifyError("Error: parameter 'object' must not be null.");
		}
		return new SingleObjectUpdater<>(object.getClass(), synClient, asyncClient,
				recordsCache, true, namespace, object);
	}

	@Override
	public MultiKeyUpdater createAll(Key[] keys, Object[] objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys == null || keys.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: array 'objects' must be same length as 'keys' array");
		}
		return new MultiKeyUpdater(synClient, asyncClient, recordsCache, true, namespace, keys, objects);
	}

	@Override
	public MultiKeyUpdater createAll(Long[] keys, Object[] objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys == null || keys.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: array 'objects' must be same length as 'keys' array");
		}
		return new MultiKeyUpdater(synClient, asyncClient, recordsCache, true, namespace, keys, objects);
	}

	@Override
	public MultiKeyUpdater createAll(String[] keys, Object[] objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys == null || keys.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: array 'objects' must be same length as 'keys' array");
		}
		return new MultiKeyUpdater(synClient, asyncClient, recordsCache, true, namespace, keys, objects);
	}

	@Override
	public MultiObjectUpdater createAll(Object... objects) {

		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		return new MultiObjectUpdater(synClient, asyncClient,
				recordsCache, true, namespace, objects);
	}

	@Override
	public <T> SingleObjectUpdater<T> update(T object) {
		if (object == null) {
			throw new SpikeifyError("Error: parameter 'object' must not be null.");
		}
		return new SingleObjectUpdater<>(object.getClass(), synClient, asyncClient,
				recordsCache, false, namespace, object);
	}

	@Override
	public <T> SingleKeyUpdater<T, Key> update(Key key, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, false, namespace, object, key);
	}

	@Override
	public <T> SingleKeyUpdater<T, Long> update(Long userKey, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, false, namespace, object, userKey);
	}

	@Override
	public <T> SingleKeyUpdater<T, String> update(String userKey, T object) {
		return new SingleKeyUpdater<>(synClient, asyncClient, recordsCache, false, namespace, object, userKey);
	}

	@Override
	public MultiObjectUpdater updateAll(Object... objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array");
		}
		return new MultiObjectUpdater(synClient, asyncClient,
				recordsCache, false, namespace, objects);
	}

	public SingleObjectDeleter delete(Object object) {
		return new SingleObjectDeleter(synClient, asyncClient, recordsCache, namespace, object);
	}

	@Override
	public MultiObjectDeleter deleteAll(Object... objects) {
		return new MultiObjectDeleter(synClient, asyncClient, recordsCache, namespace, objects);
	}

	@Override
	public SingleKeyDeleter delete(Key key) {
		return new SingleKeyDeleter(synClient, asyncClient, recordsCache, namespace, key);
	}

	@Override
	public SingleKeyDeleter delete(Long userKey) {
		return new SingleKeyDeleter(synClient, asyncClient, recordsCache, namespace, userKey);
	}

	@Override
	public SingleKeyDeleter delete(String userKey) {
		return new SingleKeyDeleter(synClient, asyncClient, recordsCache, namespace, userKey);
	}

	@Override
	public MultiKeyDeleter deleteAll(Key... keys) {
		return new MultiKeyDeleter(synClient, asyncClient, recordsCache, namespace, keys);
	}

	@Override
	public MultiKeyDeleter deleteAll(Long... keys) {
		return new MultiKeyDeleter(synClient, asyncClient, recordsCache, namespace, keys);
	}

	@Override
	public MultiKeyDeleter deleteAll(String... keys) {
		return new MultiKeyDeleter(synClient, asyncClient, recordsCache, namespace, keys);
	}

	@Override
	public <T> Scanner<T> query(Class<T> type) {
		return new Scanner<>(type, synClient, asyncClient, classConstructor, recordsCache, namespace);
	}

	@Override
	public void truncateSet(String namespace, String setName) {
		Truncater.truncateSet(namespace, setName, synClient);
	}

	@Override
	public void truncateSet(Class type) {
		ClassMapper mapper = MapperService.getMapper(type);
		String ns = mapper.getNamespace() != null ? mapper.getNamespace() : namespace;
		if (ns == null) {
			throw new SpikeifyError("Error: namespace not defined.");
		}
		String setName = mapper.getSetName();
		if (setName == null) {
			throw new SpikeifyError("Error: @SetName annotation not defined on class " + type.getName());
		}
		Truncater.truncateSet(ns, setName, synClient);
	}

	@Override
	public void truncateNamespace(String namespace) {
		Truncater.truncateNamespace(namespace, synClient);
	}

	@Override
	public <R> R transact(int limitTries, Work<R> work) {

		// log.info("Transaction_started");
		int retries = 0;
		List<String> durations = new ArrayList<>(5);
		List<String> delays = new ArrayList<>(5);
		long start = System.currentTimeMillis();
		while (true) {
			try {
				start = System.currentTimeMillis();
				R result = work.run();

				// log retries
				if (retries > 0) {
					log.info("Transaction_retry: count:" + retries + " durations:" + durations.toString() + " delays:" + delays.toString());
				}

				// success - exit the transact wrapper
				return work.onSuccess(result);
			} catch (AerospikeException ex) {
				if (retries++ < limitTries) {
					if (log.isLoggable(Level.WARNING)) {
						log.warning("Optimistic concurrency failure for " + work + " (retrying:" + retries + "): " + ex);
					}

					if (log.isLoggable(Level.FINEST)) {
						log.finest("Details of optimistic concurrency failure /n"+ex.getMessage());
					}
				} else {
					return work.onError(ex);  // let the error handler decide what to do with ConcurrentModificationException
				}
			} catch (Exception ex) {
				return work.onError(ex);   // let the error handler decide what to do with exception
			}
			try {
				long taskDuration = System.currentTimeMillis() - start;
				durations.add(String.valueOf(taskDuration));  // remember the task duration
				long delay = taskDuration + 100 + (long) (100 * Math.random());
				delays.add(String.valueOf(delay));  // remember the task duration
				Thread.sleep(delay); // sleep for random time to give competing thread chance to finish job
			} catch (InterruptedException e) {
				log.log(Level.SEVERE, "Thread.sleep() InterruptedException: ", e);
			}
		}

	}
}
