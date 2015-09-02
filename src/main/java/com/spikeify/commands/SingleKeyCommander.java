package com.spikeify.commands;

import com.aerospike.client.*;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A command chain to execute a series of atomic commands on a single Record.
 * This class is not intended to be instantiated by user.
 */
@SuppressWarnings({"SameParameterValue", "WeakerAccess"})
public class SingleKeyCommander<T> {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#command(Class)} or similar method.
	 */
	public SingleKeyCommander(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor,
	                          RecordsCache recordsCache, String defaultNamespace) {
		this.type = type;
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.policy = new WritePolicy();
		mapper = MapperService.getMapper(type);
		this.namespace = mapper.getNamespace() != null ? mapper.getNamespace() : defaultNamespace;
		this.setName = mapper.getSetName();
	}

	protected String namespace;
	protected String setName;
	protected String stringKey;
	protected Long longKey;
	protected Key key;
	protected final Class<T> type;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	protected final ClassConstructor classConstructor;
	protected final RecordsCache recordsCache;
	protected WritePolicy policy;
	protected final ClassMapper<T> mapper;
	protected final List<Operation> operations = new ArrayList<>();

	/**
	 * Sets the key of the record to be updated.
	 *
	 * @param userKey A user key of the record to be updated.
	 */
	public SingleKeyCommander<T> key(String userKey) {
		this.stringKey = userKey;
		this.longKey = null;
		this.key = null;
		return this;
	}

	/**
	 * Sets the key of the record to be updated.
	 *
	 * @param userKey A user key of the record to be updated.
	 */
	public SingleKeyCommander<T> key(Long userKey) {
		this.longKey = userKey;
		this.stringKey = null;
		this.key = null;
		return this;
	}

	/**
	 * Sets the key of the record to be updated.
	 *
	 * @param key A Key of the record to be updated.
	 */
	public SingleKeyCommander<T> key(Key key) {
		this.key = key;
		this.stringKey = null;
		this.longKey = null;
		return this;
	}

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 */
	public SingleKeyCommander<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 *
	 * @param setName The name of the set.
	 */
	public SingleKeyCommander<T> setName(String setName) {
		this.setName = setName;
		return this;
	}

	/**
	 * Sets the {@link WritePolicy} to be used when creating or updating the record in the database.
	 * <br/>Internally the 'sendKey' property of the policy will always be set to true.
	 * <br/> If this method is called within .transact() method then the 'generationPolicy' property will be set to GenerationPolicy.EXPECT_GEN_EQUAL
	 * <br/> The 'recordExistsAction' property is set accordingly depending if this is a create or update operation
	 *
	 * @param policy The policy.
	 */
	public SingleKeyCommander<T> policy(WritePolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		return this;
	}

	/**
	 * An atomic operation that adds a value to an existing bin value. Bin value must be an integer type.
	 *
	 * @param fieldName Name of the bin or, if mapped Class was provided, name of the mapped field
	 * @param value     Value to add to the bin value.
	 */
	public SingleKeyCommander<T> add(String fieldName, int value) {
		String binName = mapper != null ? mapper.getBinName(fieldName) : fieldName;
		operations.add(Operation.add(new Bin(binName, value)));
		return this;
	}

	/**
	 * An atomic operation that sets or updates a value of a bin.
	 * The provided field value will be converted to the property value, based on the converters defined via the class mapping.
	 *
	 * @param fieldName  Name of the mapped field.
	 * @param fieldValue Field value to be saved to the bin.
	 */
	public SingleKeyCommander<T> set(String fieldName, Object fieldValue) {
		String binName = mapper.getBinName(fieldName);
		Object propertyValue = mapper.getFieldMapper(fieldName).converter.fromField(fieldValue);
		operations.add(Operation.put(new Bin(binName, propertyValue)));
		return this;
	}

	/**
	 * An atomic operation that gets a value of a bin.
	 * The provided field value will be converted to the property value, based on the converters defined via the class mapping.
	 *
	 * @param fieldName Name of the mapped field.
	 */
	public SingleKeyCommander<T> get(String fieldName) {
		String binName = mapper.getBinName(fieldName);
		operations.add(Operation.get(binName));
		return this;
	}

	/**
	 * An atomic operation that sets or updates a value of a bin.
	 * The provided field value will be converted to the property value, based on the converters defined via the class mapping.
	 *
	 * @param fieldName Name of the mapped field.
	 */
	public SingleKeyCommander<T> getBytes(String fieldName, int offset, int length) {
		String binName = mapper.getBinName(fieldName);
		operations.add(Operation.get(binName));
		return this;
	}

	/**
	 * An atomic operation that appends a value to an existing bin value. Bin value must be a string type.
	 *
	 * @param fieldName Name of the bin or, if mapped Class was provided, name of the mapped field
	 * @param value     Value to append to the bin value.
	 */
	public SingleKeyCommander<T> append(String fieldName, String value) {
		String binName = mapper != null ? mapper.getBinName(fieldName) : fieldName;
		operations.add(Operation.append(new Bin(binName, value)));
		return this;
	}

	/**
	 * An atomic operation that prepends a value to an existing bin value. Bin value must be a string type.
	 *
	 * @param fieldName Name of the bin or, if mapped Class was provided, name of the mapped field
	 * @param value     Value to prepend to the bin value.
	 */
	public SingleKeyCommander<T> prepend(String fieldName, String value) {
		String binName = mapper != null ? mapper.getBinName(fieldName) : fieldName;
		operations.add(Operation.prepend(new Bin(binName, value)));
		return this;
	}

	protected void collectKeys() {

		// check if any Long or String keys were provided
		if (stringKey != null) {
			key = new Key(getNamespace(), getSetName(), stringKey);
		} else if (longKey != null) {
			key = new Key(getNamespace(), getSetName(), longKey);
		}

		if (key == null) {
			throw new SpikeifyError("Missing parameter: key not specified.");
		}
	}

	protected String getNamespace() {
		if (namespace == null) {
			throw new SpikeifyError("Namespace not set.");
		}
		return namespace;
	}

	protected String getSetName() {
		if (setName == null) {
			throw new SpikeifyError("Set name not set.");
		}
		return setName;
	}

	/**
	 * Synchronously executes a set of atomic commands.
	 *
	 * @return If there is a read operation in the set of commands, then it returns a Map of field name, field value.
	 * Otherwise returns null.
	 */
	public Map<String, Object> now() {

		if (operations.isEmpty()) {
			throw new SpikeifyError("Error missing command: at least one command method must be called: add(), append() or prepend()");
		}

		collectKeys();

		// must be set so that user key can be retrieved in queries
		this.policy.sendKey = true;

		Record rec = synClient.operate(policy, key, operations.toArray(new Operation[operations.size()]));

		if (rec == null || rec.bins == null) {
			return null;
		}

		return mapper.getFieldValues(rec.bins);
	}
}
