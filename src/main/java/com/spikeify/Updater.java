package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import java.util.Map;
import java.util.Set;

public class Updater<T> {

	private final T object;

	public Updater(Class type, T object, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor,
	               RecordsCache recordsCache, boolean create) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.create = create;
		this.policy = new WritePolicy();
		this.policy.sendKey = true;
		this.mapper = MapperService.getMapper(type);
		this.object = object;
	}

	protected String namespace;
	protected String setName;
	protected String stringKey;
	protected Long longKey;
	protected IAerospikeClient synClient;
	protected IAsyncClient asyncClient;
	protected ClassConstructor classConstructor;
	private RecordsCache recordsCache;
	private final boolean create;
	protected WritePolicy policy;
	protected ClassMapper<T> mapper;

	public Updater<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public Updater<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public Updater<T> key(String key) {
		this.stringKey = key;
		this.longKey = null;
		return this;
	}

	public Updater<T> key(long key) {
		this.longKey = key;
		this.stringKey = null;
		return this;
	}

	public Updater<T> key(Key key) {
		this.namespace = key.namespace;
		this.setName = key.setName;
		Value userKey = key.userKey;
		if (userKey instanceof Value.StringValue) {
			this.stringKey = ((Value.StringValue) userKey).toString();
		} else if (userKey instanceof Value.LongValue) {
			this.longKey = ((Value.LongValue) userKey).toLong();
		} else {
			throw new IllegalStateException("Spikeify only supports Keys created from String and Long.");
		}
		return this;
	}

	public Updater<T> policy(WritePolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		if (create) {
			this.policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		} else {
			this.policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
		}
		return this;
	}

	protected Key checkKey() {

		String useNamespace = getNamespace();
		String useSetName = getSetName();

		Key key;
		if (stringKey != null) {
			key = new Key(useNamespace, useSetName, stringKey);
		} else if (longKey != null) {
			key = new Key(useNamespace, useSetName, longKey);
		} else {
			throw new IllegalStateException("Error: missing parameter 'key'");
		}
		return key;
	}

	protected String getNamespace() {
		String useNamespace = namespace != null ? namespace : mapper.getNamespace();
		if (useNamespace == null) {
			throw new IllegalStateException("Namespace not set.");
		}
		return useNamespace;
	}

	protected String getSetName() {
		return setName != null ? setName : mapper.getSetName();
	}

	public Key now() {

		Key key = checkKey();
		String useNamespace = getNamespace();
		String useSetName = getSetName();

		Map<String, Object> props = mapper.getProperties(object);
		Set<String> changedProps = recordsCache.update(key, props);

		Bin[] bins = new Bin[changedProps.size()];
		int position = 0;
		for (String propName : changedProps) {
			bins[position++] = new Bin(propName, props.get(propName));
		}

		Long expiration = mapper.getExpiration(object);
		if (expiration != null) {
			// Entities expiration:  Java time in milliseconds
			// Aerospike expiration: seconds from 1.1.2010 = 1262304000s.
			policy.expiration = (int) (expiration / 1000) - 1262304000;
		}

		synClient.put(policy, key, bins);

		return key;
	}
}
