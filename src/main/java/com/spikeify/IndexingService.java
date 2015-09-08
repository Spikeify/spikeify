package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.spikeify.annotations.*;
import com.spikeify.commands.InfoFetcher;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Takes care of index creation and usage
 */
public class IndexingService {

	private static final List<Class> registeredEntities = new ArrayList<>();

	/**
	 * Creates indexes upon information given in {@link Indexed} annotation
	 *
	 * @param sfy     configured spikeify service
	 * @param policy  index policy if any
	 * @param clazz   entity
	 */
	public static void createIndex(Spikeify sfy, Policy policy, Class<?> clazz) {

		// skip registration if already registered
		if (registeredEntities.contains(clazz))
			return;

		registeredEntities.add(clazz);

		// look up @Indexed annotations in clazz and prepare data for indexing
		Field[] fields = clazz.getDeclaredFields();

		// get current index info (if existent)
		Map<String, InfoFetcher.IndexInfo> indexes = sfy.info().getIndexes(sfy.getNamespace());

		if (fields != null && fields.length > 0) {
			for (Field field : fields) {

				// ignored fields and keys are skipped
				if (field.getAnnotation(Ignore.class) != null ||
					field.getAnnotation(Generation.class) != null ||
					field.getAnnotation(UserKey.class) != null) {
					continue;
				}

				Indexed index = field.getAnnotation(Indexed.class);

				if (index != null) {

					if (field.getAnnotation(AsJson.class) != null) {
						throw new SpikeifyError("Field : '" + field.getName() + "' can't index fields converted to JSON, remove @AsJson or @Index annotation!");
					}

					String indexName = index.name();
					IndexType indexType = getIndexType(field);
					IndexCollectionType collectionType = getIndexCollectionType(field, index.collection());

					// if name not given ... name is generated automatically
					if ("".equals(indexName)) {
						indexName = generateIndexName(clazz, field);
					}

					// check before creating new index
					check(indexes, clazz, field.getName(), indexName, indexType, collectionType);

					// we have all the data to create the index ... let's do it
					createIndex(clazz, sfy.getClient(), policy, sfy.getNamespace(), indexName, field.getName(), indexType, collectionType);
				}
			}
		}
	}

	protected static IndexType getIndexType(Field field) {

		if (field.getType().isAssignableFrom(Character.class) ||
			field.getType().isAssignableFrom(char.class) ||
			field.getType().isAssignableFrom(BigDecimal.class) ||
			field.getType().isAssignableFrom(BigInteger.class)) {
			throw new SpikeifyError("Can't index field: " + field.getName() + ", indexing field type: " + field.getType() + " not supported!");
		}

			// others are Strings (char, enum or string)
		if (field.getType().isAssignableFrom(boolean.class) ||
			field.getType().isAssignableFrom(int.class) ||
		    field.getType().isAssignableFrom(long.class) ||
		    field.getType().isAssignableFrom(byte.class) ||
		    field.getType().isAssignableFrom(short.class) ||
		    field.getType().isAssignableFrom(double.class) ||
		    field.getType().isAssignableFrom(float.class) ||
		    field.getType().isAssignableFrom(double.class) ||
		    field.getType().isAssignableFrom(Boolean.class) ||
		    field.getType().isAssignableFrom(Integer.class) ||
			field.getType().isAssignableFrom(Long.class) ||
			field.getType().isAssignableFrom(Byte.class) ||
			field.getType().isAssignableFrom(Short.class) ||
			field.getType().isAssignableFrom(Float.class) ||
			field.getType().isAssignableFrom(Double.class)) {
			return IndexType.NUMERIC;
		}

		return IndexType.STRING;
	}

	private static IndexCollectionType getIndexCollectionType(Field field, IndexCollectionType defaultType) {

		if (!IndexCollectionType.DEFAULT.equals(defaultType)) {
			return defaultType;
		}

		if (field.getType().isAssignableFrom(List.class) ||
			field.getType().isAssignableFrom(Array.class)) {
			return IndexCollectionType.LIST;
		}

		if (field.getType().isAssignableFrom(Map.class)) {
			return IndexCollectionType.MAPKEYS;
		}

		return defaultType;
	}

	/**
	 * Checks if existing index and to be created index are clashing!
	 * @param indexes list of existing indexes in namespace
	 * @param fieldName field name
	 * @param indexName to be created index name
	 * @param indexType to be created index type
	 * @param collectionType  to be created index collection type
	 */
	private static void check(Map<String, InfoFetcher.IndexInfo> indexes, Class clazz, String fieldName, String indexName, IndexType indexType, IndexCollectionType collectionType) {

		String classSetName = getSetName(clazz);
		InfoFetcher.IndexInfo found = indexes.get(indexName);

		if (found != null) {

			if (!classSetName.equals(found.setName)) {
				throw new SpikeifyError("Index: '" + indexName + "' is already indexing entity: '" + found.setName+ "', can not bind to: '" + clazz.getName() + "'");
			}

			if (!fieldName.equals(found.fieldName)) {
				throw new SpikeifyError("Index: '" + indexName + "' is already indexing field: '" + found.fieldName + "', can not bind to: '" + fieldName + "'");
			}

			if (!indexType.equals(found.indexType)) {
				throw new SpikeifyError("Index: '" + indexName + "' can not change index type from: '" + found.indexType + "', to: '" + indexType + "', remove index manually!");
			}

			if (!collectionType.equals(found.collectionType)) {
				throw new SpikeifyError("Index: '" + indexName + "' can not change index collection type from: '" + found.collectionType+ "', to: '" + collectionType + "', remove index manually!");
			}
		}

		// reverse search ... is there some index on this field and set ?
		for (InfoFetcher.IndexInfo info: indexes.values()) {

			if (info.setName.equals(classSetName) &&
				info.fieldName.equals(fieldName) &&
				!indexName.equals(info.name)) {
				throw new SpikeifyError("Index: '" + info.name + "' is already indexing field: '" + fieldName + "' on: '" + classSetName + "', remove this index before applying: '" + indexName + "' on: '" + clazz.getName() + "'!");
			}
		}
	}

	/**
	 * Utility to generete index name from entity name and field name
	 * @param clazz entity
	 * @param field name
	 * @return index name
	 */
	static String generateIndexName(Class<?> clazz, Field field) {

		String setName = getSetName(clazz);
		return "idx_" + setName + "_" + field.getName();
	}

	public static String getSetName(Class<?> clazz) {

		SetName setName = clazz.getAnnotation(SetName.class);
		if (setName == null) {
			return clazz.getSimpleName();
		}

		return setName.value();
	}

	/**
	 * Creates index for entity field ...
	 *
	 * @param entityType entity class
	 * @param client aerospike client
	 * @param policy policy
	 * @param namespace namespace
	 * @param indexName name of index
	 * @param fieldName name of field
	 * @param indexType type of index
	 * @param collectionType type of collection index
	 * @return indexing task
	 */
	private static IndexTask createIndex(Class entityType,
										 IAerospikeClient client,
										 Policy policy,
										 String namespace,
										 String indexName,
										 String fieldName,
										 IndexType indexType,
										 IndexCollectionType collectionType) {

		if (policy == null) {
			policy = new Policy();
		}

		if (collectionType == null) {
			collectionType = IndexCollectionType.DEFAULT;
		}

		return client.createIndex(policy, namespace, getSetName(entityType), indexName, fieldName, indexType, collectionType);
	}
}
