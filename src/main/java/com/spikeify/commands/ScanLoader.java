package com.spikeify.commands;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;

import java.util.ArrayList;
import java.util.List;

/**
 * A command chain for getting all records from database.
 *
 * @param <T>
 */
public class ScanLoader<T> {

	public ScanLoader(Class<T> type,
					  IAerospikeClient synClient,
					  ClassConstructor classConstructor,
					  RecordsCache recordsCache,
					  String namespace) {

		this.synClient = synClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.namespace = namespace;

		this.policy = new ScanPolicy();
		this.policy.sendKey = true;

		this.maxRecords = 0;

		this.mapper = MapperService.getMapper(type);
		this.type = type;
	}

	protected String namespace;

	protected final IAerospikeClient synClient;
	protected final ClassConstructor classConstructor;
	protected final RecordsCache recordsCache;

	protected ScanPolicy policy;
	protected long maxRecords;

	protected final ClassMapper<T> mapper;
	protected final Class<T> type;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 */
	public ScanLoader<T> namespace(String namespace) {

		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the {@link ScanPolicy} to be used when getting the record from the database.
	 *
	 * @param policy The policy.
	 */
	public ScanLoader<T> policy(ScanPolicy policy) {

		this.policy = policy;
		this.policy.sendKey = true;
		return this;
	}

	/**
	 * Sets the maximum number of records to be retrieved
	 *
	 * @param max number of records before scan stops execution
	 */
	public ScanLoader<T> maxRecords(long max) {

		this.maxRecords = max;
		return this;
	}

	protected String getNamespace() {

		String useNamespace = namespace != null ? namespace : mapper.getNamespace();
		if (useNamespace == null) {
			throw new SpikeifyError("Namespace not set.");
		}
		return useNamespace;
	}

	protected ScanPolicy getPolicy() {

		return policy;
	}


	/**
	 * Synchronously executes multiple get commands.
	 *
	 * @return The map of Keys and Java objects mapped from records
	 */
	public List<T> now() {

		final List<T> list = new ArrayList<>();

		try {

			synClient.scanAll(getPolicy(), getNamespace(), type.getSimpleName(), new ScanCallback() {
				@Override
				public void scanCallback(Key key, Record record) throws AerospikeException {

					T object = classConstructor.construct(type);

					// save record hash into cache - used later for differential updating
					recordsCache.insert(key, record.bins);

					MapperService.map(mapper, key, record, object);

					/*// set UserKey field
					switch (key.userKey.getType()) {
						case ParticleType.STRING:
							mapper.setUserKey(object, key.userKey.toString());
							break;
						case ParticleType.INTEGER:
							mapper.setUserKey(object, key.userKey.toLong());
							break;
					}

					// set meta-fields on the entity: @Namespace, @SetName, @Expiration..
					mapper.setMetaFieldValues(object, key.namespace, key.setName, record.generation, record.expiration);

					// set field values
					mapper.setFieldValues(object, record.bins);*/

					list.add(object);

					if (maxRecords > 0 && list.size() >= maxRecords) {
						// quit scanning if we have enough
						throw new AerospikeException.ScanTerminated();
					}
				}
			});
		}
		catch (AerospikeException.ScanTerminated e) {
			// this is not the best way to do this ...
			// check if exception was thrown from ScanLoader ... if not propagate
			if (list.size() != maxRecords)
				throw e;
		}

		return list;
	}
}
