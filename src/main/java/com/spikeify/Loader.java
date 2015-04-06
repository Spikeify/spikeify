package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.policy.Policy;

public class Loader<T> implements Command<T> {

	private String namespace;
	private String setName;
	private String stringKey;
	private Long longKey;
	private AerospikeClient synCLient;
	private AsyncClient asyncClient;
	private ClassConstructor classConstructor;
	private Policy readPolicy;
	private ClassMapper<T> mapper;
	private Class<T> type;

	public Loader(Class<T> type, Loader<?> loader) {
		this.namespace = loader.namespace;
		this.setName = loader.setName;
		this.stringKey = loader.stringKey;
		this.longKey = loader.longKey;
		this.asyncClient = loader.asyncClient;
		this.synCLient = loader.synCLient;
		this.classConstructor = loader.classConstructor;
		this.readPolicy = loader.readPolicy;
		this.mapper = MapperService.getMapper(type);
		this.type = type;
	}

	public Loader(AerospikeClient synCLient, AsyncClient asyncClient, ClassConstructor classConstructor) {
		this.synCLient = synCLient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.readPolicy = new Policy();
		this.readPolicy.sendKey = true;
	}

	public <E> Loader<E> type(Class<E> type) {
		return new Loader<>(type, this);
	}

	public Loader<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public Loader<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public Loader<T> key(String key) {
		this.stringKey = key;
		this.longKey = null;
		return this;
	}

	public Loader<T> key(long key) {
		this.longKey = key;
		this.stringKey = null;
		return this;
	}

	public Loader<T> policy(Policy readPolicy) {
		this.readPolicy = readPolicy;
		this.readPolicy.sendKey = true;
		return this;
	}

	public T now() {

		Key key;
		String useNamespace = namespace != null ? namespace : mapper.getNamespace();
		if (namespace == null) {
			throw new IllegalStateException("Namespace not set.");
		}

		String useSetName = setName != null ? setName : mapper.getSetName();
		if (stringKey != null) {
			key = new Key(useNamespace, useSetName, stringKey);
		} else if (longKey != null) {
			key = new Key(useNamespace, useSetName, longKey);
		} else {
			throw new IllegalStateException("Error: missing parameter 'key'");
		}
		Record record = synCLient.get(readPolicy, key);
		T object = classConstructor.construct(type);

		mapper.setMetaFieldValues(object, useNamespace, useSetName, record.generation, record.expiration);

		mapper.setFieldValues(object, record.bins);

		return object;
	}

}
