package com.spikeify;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.query.RecordSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public class ResultSet<T> implements Iterable<T> {

	private final ClassMapper<T> mapper;
	private final ClassConstructor classConstructor;
	private final RecordsCache recordsCache;
	public final RecordSet recordSet;

	private Boolean hasNext;
	private T nextRecord;

	protected ResultSet(ClassMapper<T> mapper, ClassConstructor classConstructor,
	                    RecordsCache recordsCache, RecordSet recordSet) {
		this.mapper = mapper;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.recordSet = recordSet;
	}

	public final Key getKey() {
		return recordSet.getKey();
	}

	private T getObject() {

		Record record = recordSet.getRecord();
		Key key = recordSet.getKey();

		// construct the entity object via provided ClassConstructor
		T object = classConstructor.construct(mapper.getType());

		// save record hash into cache - used later for differential updating
		recordsCache.insert(key, record.bins);

		// set UserKey field
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
		mapper.setFieldValues(object, record.bins);

		return object;
	}

	public final void close() {
		recordSet.close();
	}

	@Override
	public Iterator<T> iterator() {
		return new Itr();
	}

	private class Itr implements Iterator<T> {

		@Override
		public boolean hasNext() {
			if (hasNext == null) {
				hasNext = recordSet.next();
				if (hasNext) {
					nextRecord = getObject();
				}
			}
			return hasNext;
		}

		@Override
		public T next() {
			hasNext = null;
			if (nextRecord == null) {
				return getObject();
			} else {
				T nextRecordRef = nextRecord;
				nextRecord = null;
				return nextRecordRef;
			}
		}

		@Override
		public void remove() {
			throw new IllegalStateException("Remove operation is not supported.");
		}
	}

	/**
	 * Generic method to convert iterator to list
	 * @return list of values or empty list if none present
	 */
	public List<T> toList() {

		List<T> output = new ArrayList<>();
		Iterator<T> iterator = iterator();

		while (iterator.hasNext()) {
			output.add(iterator.next());
		}

		close();

		return output;
	}

	/**
	 * Helper method to return only first item in result set or null if not found
	 * @return first item in list or null if no item present
	 */
	public T getFirst() {

		Iterator<T> iterator = iterator();

		if (iterator.hasNext()) {
			close();
			return iterator.next();
		}

		close();
		return null;
	}
}
