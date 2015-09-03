package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.spikeify.annotations.Ignore;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;
import com.spikeify.commands.InfoFetcher;

import java.lang.reflect.Field;
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
					field.getAnnotation(UserKey.class) != null) {
					continue;
				}

				Indexed index = field.getAnnotation(Indexed.class);
				if (index != null) {

					String indexName = index.name();
					IndexType indexType = index.type();
					IndexCollectionType collectionType = index.collection();

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

	/**
	 * Checks if existing index and to be created index are clashing!
	 * @param indexes list of existing indexes in namespace
	 * @param fieldName field name
	 * @param indexName to be created index name
	 * @param indexType to be created index type
	 * @param collectionType  to be created index collection type
	 */
	private static void check(Map<String, InfoFetcher.IndexInfo> indexes, Class clazz, String fieldName, String indexName, IndexType indexType, IndexCollectionType collectionType) {

		InfoFetcher.IndexInfo found = indexes.get(indexName);
		if (found != null) {

			if (!clazz.getSimpleName().equals(found.setName)) {
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
	}

	/**
	 * Utility to generete index name from entity name and field name
	 * @param clazz entity
	 * @param field name
	 * @return index name
	 */
	private static String generateIndexName(Class<?> clazz, Field field) {

		return "idx_" + clazz.getSimpleName() + "_" + field.getName();
	}

	/**
	 * Creates index for entity field ...
	 *
	 * @param entityType
	 * @param client
	 * @param policy
	 * @param namespace
	 * @param indexName
	 * @param fieldName
	 * @param indexType
	 * @param collectionType
	 * @return
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

		return client.createIndex(policy, namespace, entityType.getSimpleName(), indexName, fieldName, indexType, collectionType);
	}
}
