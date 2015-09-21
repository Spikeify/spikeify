package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.spikeify.annotations.BinName;
import com.spikeify.annotations.Indexed;
import com.spikeify.commands.InfoFetcher;

import java.lang.reflect.Field;

@SuppressWarnings("WeakerAccess")
public class Scanner<T> {

	protected final Class<T> type;

	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	protected final ClassConstructor classConstructor;
	protected final RecordsCache recordsCache;
	protected final ClassMapper<T> mapper;

	protected String fieldName;
	protected Field field;
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

	public Scanner<T> namespace(String namespace) {

		this.namespace = namespace;
		return this;
	}

	public Scanner<T> setName(String setName) {

		this.setName = setName;

		if (indexName != null && field != null) {
			// set name has changed ... index name must be refreshed
			setIndexName(type, setName, namespace, fieldName);
		}

		return this;
	}

	public Scanner<T> policy(QueryPolicy policy) {

		this.policy = policy;
		this.policy.sendKey = true;
		return this;
	}

	/**
	 * Sets query filters. From {@link com.aerospike.client.query.Statement#setFilters(Filter... filters) Statement.setFilters(..)}
	 *
	 * @param filters An array of filters
	 * @return This command chain
	 */
	private Scanner<T> setFilters(Filter... filters) {

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

		if (indexName == null && field != null) {
			setIndexName(type, setName, namespace, field.getName());
		}

		if (indexName == null) {
			throw new SpikeifyError("Error: Index name not defined. Missing filter() expression!");
		}
	}

	public Scanner<T> filter(String nameOfField, String fieldValue) {

		setIndexName(type, setName, namespace, nameOfField);

		IndexCollectionType collectionType = IndexingService.getIndexCollectionType(type, field.getName());

		if (IndexCollectionType.DEFAULT.equals(collectionType)) {
			return setFilters(Filter.equal(fieldName, fieldValue));
		}

		return setFilters(Filter.contains(fieldName, collectionType, fieldValue));
	}

	public Scanner<T> filter(String nameOfField, long fieldValue) {

		setIndexName(type, setName, namespace, nameOfField);

		IndexCollectionType collectionType = IndexingService.getIndexCollectionType(type, field.getName());

		if (IndexCollectionType.DEFAULT.equals(collectionType)) {
			return setFilters(Filter.equal(fieldName, fieldValue));
		}

		return setFilters(Filter.contains(fieldName, collectionType, fieldValue));
	}

	public Scanner<T> filter(String nameOfField, long begin, long end) {

		setIndexName(type, setName, namespace, nameOfField);

		IndexCollectionType collectionType = IndexingService.getIndexCollectionType(type, field.getName());

		if (IndexCollectionType.DEFAULT.equals(collectionType)) {
			return setFilters(Filter.range(fieldName, begin, end));
		}

		return setFilters(Filter.range(fieldName, collectionType, begin, end));
	}

	private void setIndexName(Class<T> type, String setName, String namespace, String nameOfField) {

		// set correct global field and index name
		field = findField(nameOfField);
		fieldName = IndexingService.getFieldName(field);
		indexName = getIndexName(setName, namespace, type, field);
	}

	private Field findField(String nameOfField) {

		Field foundField = null;
		try {
			foundField = type.getDeclaredField(nameOfField);
		}
		catch (NoSuchFieldException e) {

			// try again by binName ... if found
			Field[] allFields = type.getDeclaredFields();
			for (Field member : allFields) {
				BinName found = member.getAnnotation(BinName.class);
				if (found != null && nameOfField.equals(found.value())) {
					// we have found the correct field ...
					foundField = member;
					break;
				}
			}
		}

		if (foundField == null) {
			throw new SpikeifyError("Can't query: no such field: '" + nameOfField + "' in: '" + type.getName() + "'!");
		}

		return foundField;
	}

	/**
	 * Returns set index name or generated if no name given
	 *
	 * @param setName   custom set name
	 * @param namespace custom namespace
	 * @param type      class
	 * @param field     field
	 * @return index name
	 */
	private String getIndexName(String setName, String namespace, Class type, Field field) {

		if (setName != null) {
			// explicit set name filtering (index was created manually) ... @Indexed annotation is ignored
			// index name can not be resolved from annotations we must look up in information
			InfoFetcher.IndexInfo info = IndexingService.findIndex(synClient, namespace, setName, field);

			if (info == null) {
				throw new SpikeifyError("Index in namespace: " + namespace + ", for set: " + setName + " and field: " + field.getName() + ", not found!");
			}

			return info.name;
		}

		Indexed indexed = field.getAnnotation(Indexed.class);
		if (indexed == null) {
			throw new SpikeifyError("Can't query: missing @Indexed annotation on: '" + field.getName() + "' in: '" + type.getName() + "'!");
		}

		String name = indexed.name();

		if ("".equals(name)) {
			return IndexingService.generateIndexName(type, field);
		}

		return name;
	}
}
