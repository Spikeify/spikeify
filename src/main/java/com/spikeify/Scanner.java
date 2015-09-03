package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.spikeify.annotations.Indexed;

import java.lang.reflect.Field;

@SuppressWarnings("WeakerAccess")
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

	@Deprecated
	public Scanner<T> indexName(String indexName) {
		this.indexName = indexName;
		return this;
	}

	public Scanner<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	@Deprecated
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
	 * @param filters An array of filters
	 * @return This command chain
	 */
	@Deprecated // make private
	public Scanner<T> setFilters(Filter... filters) {
		this.filters = filters;
		return this;
	}

	public ResultSet<T> now() {

		collectMetaData();

		Statement statement = new Statement();
		statement.setIndexName(indexName);
		statement.setNamespace(namespace);
		statement.setSetName(setName);
		statement.setFilters(filters);

		RecordSet recordSet = synClient.query(policy, statement);

		return new ResultSet<>(mapper, classConstructor, recordsCache, recordSet);
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

	public Scanner<T> filter(String fieldName, String fieldValue) {
		setIndexName(type, fieldName);
		return setFilters(Filter.equal(fieldName, fieldValue));
	}

	public Scanner<T> filter(String fieldName, long fieldValue) {
		setIndexName(type, fieldName);
		return setFilters(Filter.equal(fieldName, fieldValue));
	}

	public Scanner<T> filter(String fieldName, IndexCollectionType collectionType, String fieldValue) {
		setIndexName(type, fieldName);
		return setFilters(Filter.contains(fieldName, collectionType, fieldValue));
	}

	public Scanner<T> filter(String fieldName, IndexCollectionType collectionType, long fieldValue) {
		setIndexName(type, fieldName);
		return setFilters(Filter.contains(fieldName, collectionType, fieldValue));
	}

	public Scanner<T> filter(String fieldName, long begin, long end) {
		setIndexName(type, fieldName);
		return setFilters(Filter.range(fieldName, begin, end));
	}

	public Scanner<T> filter(String fieldName, IndexCollectionType collectionType, long begin, long end) {
		setIndexName(type, fieldName);
		return setFilters(Filter.range(fieldName, collectionType, begin, end));
	}

	private void setIndexName(Class<T> type, String fieldName) {
		try {
			Field field = type.getDeclaredField(fieldName);
			Indexed indexed = field.getAnnotation(Indexed.class);
			if (indexed == null) {
				throw new SpikeifyError("Can't query: missing @Indexed annotation on: '" + fieldName + "' in: '" + type.getName() + "'!");
			}

			indexName = getIndexName(indexed.name(), type, field);
			setName = type.getSimpleName();
		}
		catch (NoSuchFieldException e) {
			throw new SpikeifyError("Can't query: no such field: '" + fieldName + "' in: '" + type.getName() + "'!");
		}
	}

	/**
	 * Returns set index name or generated if no name given
	 * @param name name given in annotation
	 * @param type class
 	 * @param field field
	 * @return index name
	 */
	private String getIndexName(String name, Class type, Field field) {

		if ("".equals(name)) {
			return IndexingService.generateIndexName(type, field);
		}

		return name;
	}
}
