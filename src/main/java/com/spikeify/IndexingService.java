package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.spikeify.annotations.Ignore;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Takes care of index creation and usage
 */
public class IndexingService {

	private static final List<Class> registeredEntities = new ArrayList<>();

	/**
	 * Creates indexes upon information given in {@link Indexed} annotation
	 *
	 * @param client    configured aerospike client
	 * @param policy
	 * @param namespace namespace
	 * @param clazz     entity   @return index task or null if no task started
	 */
	public static void createIndex(IAerospikeClient client, Policy policy, String namespace, Class<?> clazz) {

		// skip registration if already registered
		if (registeredEntities.contains(clazz))
			return;

		registeredEntities.add(clazz);

		// look up @Indexed annotations in clazz and prepare data for indexing
		Field[] fields = clazz.getDeclaredFields();

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

					// we have all the data to create the index ... let's do it
					createIndex(clazz, client, policy, namespace, indexName, field.getName(), indexType, collectionType);
				}
			}
		}
	}

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
