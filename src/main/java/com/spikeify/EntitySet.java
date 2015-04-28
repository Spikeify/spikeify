package com.spikeify;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.query.RecordSet;

import java.util.Iterator;

public class EntitySet<T> implements Iterable<T> {

	private final ClassMapper<T> mapper;
	private final ClassConstructor classConstructor;
	private final RecordsCache recordsCache;
	public final RecordSet recordSet;

	private Boolean hasNext;
	private T nextRecord;

	protected EntitySet(ClassMapper<T> mapper, ClassConstructor classConstructor,
	                    RecordsCache recordsCache, RecordSet recordSet) {
		this.mapper = mapper;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.recordSet = recordSet;
	}


	public final T next() {

		if (nextRecord == null) {
			hasNext = null;
			return getObject();
		} else {
			T nextRecordRef = nextRecord;
			nextRecord = null;
			return nextRecordRef;
		}
	}

	public final void close() {
		recordSet.close();
	}

	public final Key getKey() {
		return recordSet.getKey();
	}

	private final T getObject() {

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

		// set metafields on the entity: @Namespace, @SetName, @Expiration..
		mapper.setMetaFieldValues(object, key.namespace, key.setName, record.generation, record.expiration);

		// set field values
		mapper.setFieldValues(object, record.bins);

		return object;
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
				if(hasNext){
					nextRecord = getObject();
				}
			}
			return hasNext;
		}

		@Override
		public T next() {
			if (nextRecord == null) {
				hasNext = null;
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
}
