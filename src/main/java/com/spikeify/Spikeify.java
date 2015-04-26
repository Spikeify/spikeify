package com.spikeify;

import com.aerospike.client.Key;
import com.spikeify.commands.*;

/**
 * This is the main class of Spikeify. All command chains start from this class.
 */
public interface Spikeify {

	/**
	 * Starts a command chain for info commands.
	 *
	 * @return the command chain
	 */
	InfoFetcher info();

	/**
	 * Starts a command chain for loading a single record.
	 *
	 * @param type The class to map resulting record to.
	 * @return the command chain
	 */
	<T> SingleLoader<T> get(Class<T> type);

	/**
	 * Starts a command chain for loading multiple records.
	 *
	 * @param type The class to map resulting records to.
	 * @param keys {@link Key}(s) to load from DB.
	 * @return the command chain
	 */
	<T> MultiLoader<T, Key> getAll(Class<T> type, Key... keys);

	/**
	 * Starts a command chain for loading multiple records.
	 *
	 * @param type     The class to map resulting records to.
	 * @param userKeys User key(s) of the records to load.
	 * @return the command chain
	 */
	<T> MultiLoader<T, Long> getAll(Class<T> type, Long... userKeys);

	/**
	 * Starts a command chain for loading multiple records.
	 *
	 * @param type     The class to map resulting records to.
	 * @param userKeys User key(s) of the records to load.
	 * @return the command chain
	 */
	<T> MultiLoader<T, String> getAll(Class<T> type, String... userKeys);

	/**
	 * Starts a command chain for creating a single record. This command fails if the records is already present in the DB.
	 *
	 * @param key    A {@link Key} of the record to create.
	 * @param entity The Java object to be mapped to the created record.
	 * @return the command chain
	 */
	<T> SingleKeyUpdater<T, Key> create(Key key, T entity);

	/**
	 * Starts a command chain for creating a single record. This command fails if the record already exists in the DB.
	 *
	 * @param userKey A user key of the record to create.
	 * @param entity  The Java object to be mapped to the created record.
	 * @return the command chain
	 */
	<T> SingleKeyUpdater<T, Long> create(Long userKey, T entity);

	/**
	 * Starts a command chain for creating a single record. This command fails if the record already exists  in the DB.
	 *
	 * @param userKey A user key of the record to create.
	 * @param entity  The Java object to be mapped to the created record.
	 * @return the command chain
	 */
	<T> SingleKeyUpdater<T, String> create(String userKey, T entity);

	/**
	 * Starts a command chain for creating a single record. This command fails if the record already exists in the DB.
	 *
	 * @param entity The Java object to be mapped to the created record. The Class must contain @UserKey annotation.
	 * @return the command chain
	 */
	<T> SingleObjectUpdater<T> create(T entity);

	/**
	 * Starts a command chain for creating multiple records. This command fails if any record already exists in the DB.
	 *
	 * @param keys    An array of {@link Key} of the records to create.
	 * @param objects Java objects to be mapped to the created records.
	 * @return the command chain
	 */
	MultiKeyUpdater createAll(Key[] keys, Object[] objects);

	/**
	 * Starts a command chain for creating multiple records. This command fails if any record already exists in the DB.
	 *
	 * @param userKeys An array of user keys of the records to create.
	 * @param objects  Java objects to be mapped to the created records.
	 * @return the command chain
	 */
	MultiKeyUpdater createAll(Long[] userKeys, Object[] objects);

	/**
	 * Starts a command chain for creating multiple records. This command fails if any record already exists in the DB.
	 *
	 * @param userKeys An array of user keys of the records to create.
	 * @param objects  Java objects to be mapped to the created records.
	 * @return the command chain
	 */
	MultiKeyUpdater createAll(String[] userKeys, Object[] objects);

	/**
	 * Starts a command chain for creating multiple records. This command fails if any record already exists in the DB.
	 *
	 * @param entity Java object(s) to be mapped to the created records. The Classes of objects must contain @UserKey annotation.
	 * @return the command chain
	 */
	MultiObjectUpdater createAll(Object... entity);

	/**
	 * Starts a command chain for creating or updating a single record.
	 *
	 * @param object The Java object to be mapped to the created record. The Class must contain @UserKey annotation.
	 * @return the command chain
	 */
	<T> SingleObjectUpdater<T> update(T object);

	/**
	 * Starts a command chain for creating or updating a single record.
	 *
	 * @param key    A {@link Key} of the record to create or update.
	 * @param entity The Java object to be mapped to the created record.
	 * @return the command chain
	 */
	<T> SingleKeyUpdater<T, Key> update(Key key, T entity);

	/**
	 * Starts a command chain for creating or updating a single record.
	 *
	 * @param key    A user key of the record to create or update.
	 * @param entity The Java object to be mapped to the created record.
	 * @return the command chain
	 */
	<T> SingleKeyUpdater<T, Long> update(Long key, T entity);

	/**
	 * Starts a command chain for creating or updating a single record.
	 *
	 * @param key    A user key of the record to create or update.
	 * @param entity The Java object to be mapped to the created record.
	 * @return the command chain
	 */
	<T> SingleKeyUpdater<T, String> update(String key, T entity);

	/**
	 * Starts a command chain for creating or updating multiple records.
	 *
	 * @param object Java object(s) to be mapped to the created records. The Classes of objects must contain @UserKey annotation.
	 * @return the command chain
	 */
	MultiObjectUpdater updateAll(Object... object);

	/**
	 * Starts a command chain for deleting a single record.
	 *
	 * @param object The Java object representing a record to be deleted. The Class must contain @UserKey annotation.
	 * @return the command chain
	 */
	SingleObjectDeleter delete(Object object);

	/**
	 * Starts a command chain for deleting a single record.
	 *
	 * @param key A {@link Key} of the record to delete.
	 * @return the command chain
	 */
	SingleKeyDeleter delete(Key key);

	/**
	 * Starts a command chain for deleting a single record.
	 *
	 * @param userKey A user key of the record to delete.
	 * @return the command chain
	 */
	SingleKeyDeleter delete(Long userKey);

	/**
	 * Starts a command chain for deleting a single record.
	 *
	 * @param userKey A user key of the record to delete.
	 * @return the command chain
	 */
	SingleKeyDeleter delete(String userKey);

	/**
	 * Starts a command chain for deleting multiple record.
	 *
	 * @param object Java object(s) representing record(s) to be deleted. The Classes must contain @UserKey annotation.
	 * @return the command chain
	 */
	MultiObjectDeleter deleteAll(Object... object);

	/**
	 * Starts a command chain for deleting multiple record.
	 *
	 * @param keys {@link Key}(s) of the record(s) to delete.
	 * @return the command chain
	 */
	MultiKeyDeleter deleteAll(Key... keys);

	/**
	 * Starts a command chain for deleting multiple record.
	 *
	 * @param userKeys User key(s) of the record(s) to delete.
	 * @return the command chain
	 */
	MultiKeyDeleter deleteAll(Long... userKeys);

	/**
	 * Starts a command chain for deleting multiple record.
	 *
	 * @param userKeys User key(s) of the record(s) to delete.
	 * @return the command chain
	 */
	MultiKeyDeleter deleteAll(String... userKeys);

	/**
	 * Starts a command chain to start a query.
	 *
	 * @param type The type (set name) of the records to query for.
	 * @return the command chain
	 */
	<T> Scanner<T> query(Class<T> type);

	/**
	 * Starts a command chain to delete all records with given SetName.
	 *
	 * @param namespace The namespace of the records to delete.
	 * @param setName The SetName of the records to delete.
	 * @return the command chain
	 */
	void truncateSet(String namespace, String setName);

	/**
	 * Starts a command chain to delete all records with given SetName.
	 *
	 * @param type The Class of the records to delete. Must contain a @SetName on class definition.
	 * @return the command chain
	 */
	void truncateSet(Class type);

	/**
	 * Starts a command chain to delete all records with given namespace.
	 *
	 * @param namespace The namespace of the records to delete.
	 * @return the command chain
	 */
	void truncateNamespace(String namespace);

	/**
	 * A lightweight "transaction" helper that builds upon the Aerospike's record versioning functionality.
	 * Within a unit of work you can execute a number of get and update operations.
	 * If the version of the record for any update operation mismatches, this update operation is aborted and the whole unit of work is retried.
	 * Note: this are NOT true (serialized) transactions, because:
	 * <ol>
	 *   <li>Does not take into account the read records that changed during transaction.</li>
	 *   <li>Aborts the transaction on ANY mismatched update operation, but does not rollback update operations that happened beforehand. </li>
	 * </ol>
	 * Best practice: make a series of read operations and then only one update operation.
	 * @param retries Number of retries of transaction operation. A sensible number would be 5.
	 * @param work An IDEMPOTENT unit of work (series of DB operations) that will be transactionally executed.
	 *              In case of failure, the whole set of operations will be executed again,
	 *              hence the need fot this set of operations to be IDEMPOTENT.
	 * @return R The result of the work performed by Work.run() method.
	 */
	<R> R transact(int retries, Work<R> work);
}
