package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

public class Updater<T>{

	private final T object;

	public Updater(Class type, T object, AerospikeClient synClient, AsyncClient asyncClient, ClassConstructor classConstructor, boolean create) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
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
	protected AerospikeClient synClient;
	protected AsyncClient asyncClient;
	protected ClassConstructor classConstructor;
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
			throw  new IllegalStateException("Spikeify only supports Keys created from String and Long.");
		}
		return this;
	}

	public Updater<T> policy(WritePolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		if(create){
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

		Bin[] bins = new Bin[props.size()];
		int position = 0;
		for (Map.Entry<String, Object> prop : props.entrySet()) {
			bins[position++] = new Bin(prop.getKey(), prop.getValue());
		}

		Long expiration = mapper.getExpiration(object);
		if (expiration != null) {
			// Entities expiration:  Java time in milliseconds
			// Aerospike expiration: seconds from 1.1.2010 = 1262304000s.
			policy.expiration = (int)(expiration/1000) - 1262304000;
		}

		mapper.getNamespace();

		synClient.put(policy, key, bins);
		return key;
	}
}
