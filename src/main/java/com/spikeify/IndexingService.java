package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.spikeify.annotations.*;
import com.spikeify.commands.InfoFetcher;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Takes care of index creation and usage
 */
public class IndexingService {

	private static final List<String> registeredEntities = new ArrayList<>();

	/**
	 * Creates indexes upon information given in {@link Indexed} annotation
	 *
	 * @param sfy    configured spikeify service
	 * @param policy index policy if any
	 * @param clazz  entity
	 */
	public static void createIndex(Spikeify sfy, Policy policy, Class<?> clazz) {

		// skip registration if already registered
		if (registeredEntities.contains(clazz.getName())) { return; }

		registeredEntities.add(clazz.getName());

		// look up @Indexed annotations in clazz and prepare data for indexing
		Field[] fields = clazz.getDeclaredFields();

		// get current index info (if existent)
		Map<String, InfoFetcher.IndexInfo> indexes = sfy.info().getIndexes(sfy.getNamespace());

		if (fields != null && fields.length > 0) {
			for (Field field : fields) {

				// ignored fields and keys are skipped
				if (field.isAnnotationPresent(Ignore.class) ||
					field.isAnnotationPresent(Generation.class) ||
					field.isAnnotationPresent(UserKey.class)) {
					continue;
				}

				Indexed index = field.getAnnotation(Indexed.class);

				if (index != null) {

					if (field.isAnnotationPresent(AsJson.class)) {
						throw new SpikeifyError("Field : '" + field.getName() + "' can't index fields annotated with @AsJson. Add @AsJson to class declaration or remove @Indexed annotation!");
					}

					String indexName = index.name();
					IndexType indexType = getIndexType(field);
					IndexCollectionType collectionType = getIndexCollectionType(field, index.collection());

					// if name not given ... name is generated automatically
					if ("".equals(indexName)) {
						indexName = generateIndexName(clazz, field);
					}

					// check before creating new index
					InfoFetcher.IndexInfo found = check(indexes, clazz, field, indexName, indexType, collectionType, sfy);

					// only create index if not already created
					if (found == null) {
						// we have all the data to create the index ... let's do it
						createIndex(clazz, sfy.getClient(), policy, sfy.getNamespace(), indexName, field, indexType, collectionType);
					}
				}
			}
		}
	}

	public static boolean isRegistered(Class<?> clazz) {

		return registeredEntities.contains(clazz.getName());
	}

	/**
	 * Resolved index type according to field type
	 *
	 * @param field to inspect
	 * @return STRING or NUMERIC index type
	 * @throws SpikeifyError in case indexing field type is not supported
	 */
	protected static IndexType getIndexType(Field field) {

		if (Character.class.isAssignableFrom(field.getType()) ||
			char.class.isAssignableFrom(field.getType()) ||
			BigDecimal.class.isAssignableFrom(field.getType()) ||
			BigInteger.class.isAssignableFrom(field.getType())) {
			throw new SpikeifyError("Can't index field: '" + field.getName() + "', indexing field type: '" + field.getType() + "' not supported!");
		}

		if (Collection.class.isAssignableFrom(field.getType())) {
			// we have a collection ... index is inner type
			Type type = field.getGenericType();

			if (type instanceof ParameterizedType) {
				ParameterizedType pt = (ParameterizedType) type;
				if (pt.getActualTypeArguments().length == 1) {
					return getIndexType((Class<?>) pt.getActualTypeArguments()[0]);
				}

				throw new SpikeifyError("Can't index field: '" + field.getName() + "', collection with multiple types: '" + field.getType() + "' not supported!");
			}
		}

		return getIndexType(field.getType());
	}

	private static IndexType getIndexType(Class<?> field) {

		if (field.isAssignableFrom(boolean.class) ||
			field.isAssignableFrom(int.class) ||
			field.isAssignableFrom(long.class) ||
			field.isAssignableFrom(byte.class) ||
			field.isAssignableFrom(short.class) ||
			field.isAssignableFrom(float.class) ||
			field.isAssignableFrom(double.class) ||
			field.isAssignableFrom(Boolean.class) ||
			field.isAssignableFrom(Integer.class) ||
			field.isAssignableFrom(Long.class) ||
			field.isAssignableFrom(Byte.class) ||
			field.isAssignableFrom(Short.class) ||
			field.isAssignableFrom(Float.class) ||
			field.isAssignableFrom(Double.class)) { return IndexType.NUMERIC; }

		return IndexType.STRING;
	}


	/**
	 * Resolves index collection type according to field type
	 *
	 * @param field       to inspect
	 * @param defaultType default type (override)
	 * @return index collection type for given field
	 */
	private static IndexCollectionType getIndexCollectionType(Field field, IndexCollectionType defaultType) {

		if (!IndexCollectionType.DEFAULT.equals(defaultType)) {
			return defaultType;
		}

		if (Collection.class.isAssignableFrom(field.getType()) ||
			List.class.isAssignableFrom(field.getType()) ||
			Array.class.isAssignableFrom(field.getType()) ||
			Set.class.isAssignableFrom(field.getType())) {
			return IndexCollectionType.LIST;
		}

		if (!Map.class.isAssignableFrom(field.getType())) {
			return defaultType;
		}

		return IndexCollectionType.MAPKEYS;
	}

	/**
	 * Checks if existing index and to be created index are clashing!
	 *
	 * @param indexes        list of existing indexes in namespace
	 * @param field          field name
	 * @param indexName      to be created index name
	 * @param indexType      to be created index type
	 * @param collectionType to be created index collection type
	 */
	private static InfoFetcher.IndexInfo check(Map<String, InfoFetcher.IndexInfo> indexes, Class clazz, Field field, String indexName, IndexType indexType, IndexCollectionType collectionType,
	                                           Spikeify sfy) {

		String classSetName = getSetName(clazz);
		InfoFetcher.IndexInfo found = indexes.get(indexName);

		String fieldName = getFieldName(field);

		if (found != null) {

			if (!classSetName.equals(found.setName)) {
				throw new SpikeifyError("Index: '" + indexName + "' is already indexing entity: '" + found.setName + "', can not bind to: '" + clazz.getName() + "'");
			}

			if (!fieldName.equals(found.fieldName)) {
				throw new SpikeifyError("Index: '" + indexName + "' is already indexing field: '" + found.fieldName + "', can not bind to: '" + fieldName + "'");
			}

			if (!indexType.equals(found.indexType)) {
				throw new SpikeifyError("Index: '" + indexName + "' can not change index type from: '" + found.indexType + "', to: '" + indexType + "', remove index manually: " +
					"DROP INDEX " + sfy.getNamespace() + "." + getSetName(clazz) + " " + indexName);
			}

			if (!collectionType.equals(found.collectionType)) {
				throw new SpikeifyError("Index: '" + indexName + "' can not change index collection type from: '" + found.collectionType + "', to: '" + collectionType + "', remove index manually: " +
					"DROP INDEX " + sfy.getNamespace() + "." + getSetName(clazz) + " " + indexName);
			}
		}

		// reverse search ... is there some index on this field and set ?
		for (InfoFetcher.IndexInfo info : indexes.values()) {

			if (info != null &&
				info.setName.equals(classSetName) &&
				info.fieldName.equals(fieldName) &&
				!indexName.equals(info.name)) {
				throw new SpikeifyError(
					"Index: '" + info.name + "' is already indexing field: '" + fieldName + "' on: '" + classSetName + "', remove this index before applying: '" + indexName + "' on: '" + clazz
						.getName() + "', " +
						"DROP INDEX " + sfy.getNamespace() + "." + getSetName(clazz) + " " + info.name);
			}
		}

		return found;
	}

	/**
	 * Utility to generete index name from entity name and field name
	 *
	 * @param clazz entity
	 * @param field name
	 * @return index name
	 */
	static String generateIndexName(Class<?> clazz, Field field) {

		String setName = getSetName(clazz);
		String fieldName = getFieldName(field);

		return "idx_" + setName + "_" + fieldName;
	}

	/**
	 * Return set name from annotation or class name if no annotation
	 *
	 * @param clazz to search for annotation @SetName
	 * @return set name
	 */
	public static String getSetName(Class<?> clazz) {

		SetName setName = clazz.getAnnotation(SetName.class);
		if (setName == null) {
			return clazz.getSimpleName();
		}

		return setName.value();
	}

	static String getFieldName(Field field) {

		BinName binName = field.getAnnotation(BinName.class);
		if (binName == null) {
			return field.getName();
		}

		return binName.value();
	}

	/**
	 * Creates index for entity field ...
	 *
	 * @param entityType     entity class
	 * @param client         aerospike client
	 * @param policy         policy
	 * @param namespace      namespace
	 * @param indexName      name of index
	 * @param field          name of field
	 * @param indexType      type of index
	 * @param collectionType type of collection index
	 * @return indexing task
	 */
	private static IndexTask createIndex(Class entityType,
	                                     IAerospikeClient client,
	                                     Policy policy,
	                                     String namespace,
	                                     String indexName,
	                                     Field field,
	                                     IndexType indexType,
	                                     IndexCollectionType collectionType) {

		if (policy == null) {
			policy = new Policy();
		}

		if (collectionType == null) {
			collectionType = IndexCollectionType.DEFAULT;
		}

		return client.createIndex(policy, namespace, getSetName(entityType), indexName, getFieldName(field), indexType, collectionType);
	}

	/**
	 * Gets index collection type from @Indexed annotation of given field in type
	 *
	 * @param type      to search for field
	 * @param fieldName to find in type
	 * @return index collection type or IndexCollectionType.DEFAULT if not found
	 */
	public static IndexCollectionType getIndexCollectionType(Class type, String fieldName) {

		try {
			Field field = type.getDeclaredField(fieldName);
			Indexed indexed = field.getAnnotation(Indexed.class);

			if (indexed != null) { return getIndexCollectionType(field, indexed.collection()); }

		}
		catch (NoSuchFieldException e) {
			throw new SpikeifyError("Field: '" + fieldName + "' is not present in Entity: " + type.getName());
		}

		return IndexCollectionType.DEFAULT;
	}

	/**
	 * Finds index if set name was manually changed ... custom
	 *
	 * @param asynClient client
	 * @param namespace namespace
	 * @param field     field
	 * @param setName   to search for index
	 * @return index information or null if not found
	 */
		public static InfoFetcher.IndexInfo findIndex(IAsyncClient asynClient, String namespace, String setName, Field field) {

		InfoFetcher fetcher = new InfoFetcher(asynClient);
		String fieldName = getFieldName(field);
		return fetcher.findIndex(namespace, setName, fieldName);
	}
}
