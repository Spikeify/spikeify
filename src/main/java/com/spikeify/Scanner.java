package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

public class Scanner<T> {

	protected final Class<T> type;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	protected final ClassConstructor classConstructor;
	protected final RecordsCache recordsCache;
	protected final ClassMapper<T> mapper;
	protected String indexName;
	protected String namespace;
	protected String setName;
	protected QueryPolicy policy = new QueryPolicy();
	protected Filter[] filters;

	public Scanner(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor,
	               RecordsCache recordsCache, String defaultNamespace) {
		this.type = type;
		this.mapper = MapperService.getMapper(type);
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
	}

	public Scanner<T> indexName(String indexName) {
		this.indexName = indexName;
		return this;
	}

	public Scanner<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public Scanner<T> setName(String setName) {
		this.setName = setName;
		return this;
	}

	public Scanner<T> policy(QueryPolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		return this;
	}

	/**
	 * Sets query filters. From {@link com.aerospike.client.query.Statement#setFilters(Filter... filters) Statement.setFilters(..)}
	 * @param filters
	 * @return
	 */
	public Scanner<T> setFilters(Filter... filters) {
		this.filters = filters;
		return this;
	}

	public EntitySet<T> now() {

		collectMetaData();

		Statement statement = new Statement();
		statement.setNamespace(namespace);
		statement.setSetName(setName);
		statement.setFilters(filters);

		RecordSet recordSet = synClient.query(policy, statement);

		return new EntitySet<>(mapper, classConstructor, recordsCache, recordSet);
	}

	protected void collectMetaData() {

		if (mapper.getNamespace() != null) {
			namespace = mapper.getNamespace();
		}

		if (setName == null && mapper.getSetName() != null) {
			setName = mapper.getSetName();
		}

		if (setName == null) {
			throw new SpikeifyError("Error: SetName not defined.");
		}

		if (indexName == null) {
			throw new SpikeifyError("Error: Index name not defined.");
		}
	}

}
