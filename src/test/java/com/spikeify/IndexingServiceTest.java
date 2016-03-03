package com.spikeify;

import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.spikeify.commands.InfoFetcher;
import com.spikeify.entity.*;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class IndexingServiceTest extends SpikeifyTest {

	public IndexingServiceTest() {
		dropIndexesOnCleanup = false;
	}

	@Test
	public void readInfoNonExistent() {

		Map<String, InfoFetcher.IndexInfo> indexes = sfy.info().getIndexes(namespace, EntityTwo.class);
		assertEquals(0, indexes.size());

		SpikeifyService.register(EntityTwo.class); // nothing to register
	}

	@Test
	public void testCreateIndex() throws Exception {

		// create index ...
		IndexingService.createIndex(sfy, new Policy(), EntityOne.class);
		IndexingService.createIndex(sfy, new Policy(), EntityIndexed.class);

		// check if index exists ...
		Map<String, InfoFetcher.IndexInfo> indexes = sfy.info().getIndexes(namespace, EntityOne.class);
		assertEquals(4, indexes.size());

		InfoFetcher.IndexInfo info = indexes.get("index_one");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("index_one", info.name);
		assertEquals("EntityOne", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		//
		info = indexes.get("idx_EntityOne_two");
		assertNotNull(info);
		assertEquals(IndexType.STRING, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("idx_EntityOne_two", info.name);
		assertEquals("EntityOne", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		//
		info = indexes.get("idx_EntityOne_nine");
		assertNotNull(info);
		assertEquals(IndexType.STRING, info.indexType);
		assertEquals(IndexCollectionType.LIST, info.collectionType);
		assertEquals("idx_EntityOne_nine", info.name);
		assertEquals("EntityOne", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		//
		info = indexes.get("idx_EntityOne_seven");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("idx_EntityOne_seven", info.name);
		assertEquals("EntityOne", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		// 3.
		indexes = sfy.info().getIndexes(namespace, EntityIndexed.class);
		assertEquals(20, indexes.size());

		info = indexes.get("idx_EntityIndexed_text");
		assertNotNull(info);
		assertEquals(IndexType.STRING, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("idx_EntityIndexed_text", info.name);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("index_number");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("index_number", info.name);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_list");
		assertNotNull(info);
		assertEquals(IndexType.STRING, info.indexType);
		assertEquals(IndexCollectionType.LIST, info.collectionType);
		assertEquals("idx_EntityIndexed_list", info.name);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_items");
		assertNotNull(info);
		assertEquals(IndexType.STRING, info.indexType);
		assertEquals(IndexCollectionType.LIST, info.collectionType);
		assertEquals("idx_EntityIndexed_items", info.name);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);


		info = indexes.get("idx_EntityIndexed_mapKeys");
		assertNotNull(info);
		assertEquals(IndexType.STRING, info.indexType);
		assertEquals(IndexCollectionType.MAPKEYS, info.collectionType);
		assertEquals("idx_EntityIndexed_mapKeys", info.name);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_mapValues");
		assertNotNull(info);
		assertEquals(IndexType.STRING, info.indexType);
		assertEquals(IndexCollectionType.MAPVALUES, info.collectionType);
		assertEquals("idx_EntityIndexed_mapValues", info.name);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		//
		info = indexes.get("idx_EntityIndexed_aboolean");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_along");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_abyte");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_ashort");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_adouble");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_afloat");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_aBoolean");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_aInteger");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_aLong");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_aByte");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_aShort");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_aFloat");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);

		info = indexes.get("idx_EntityIndexed_aDouble");
		assertNotNull(info);
		assertEquals(IndexType.NUMERIC, info.indexType);
		assertEquals(IndexCollectionType.DEFAULT, info.collectionType);
		assertEquals("EntityIndexed", info.setName);
		assertEquals(namespace, info.namespace);
		assertTrue(info.canRead);
		assertTrue(info.canWrite);
		assertTrue(info.synced);
	}

	public String stringField;

	public int intField;

	public long longField;

	public byte byteField;

	public short shortField;

	public float floatField;

	public double aDoubleField;

	public Boolean aBooleanField;

	public Integer aIntegerField;

	public Long aLongField;

	public Byte aByteField;

	public Short aShortField;

	public Float aFloatField;

	public Double doubleField;

	public List<Long> longList;

	public Collection<Long> longCollection;

	public List<String> stringList;

	public char charField;
	public Character aCharField;


	@Test
	public void testResolveIndexType() throws NoSuchFieldException {

		assertEquals(IndexType.STRING, IndexingService.getIndexType(getClass().getField("stringField")));

		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("intField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("longField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("byteField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("shortField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("floatField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("aDoubleField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("aBooleanField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("aIntegerField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("aLongField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("aByteField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("aShortField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("aFloatField")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("doubleField")));

		assertEquals(IndexType.STRING, IndexingService.getIndexType(getClass().getField("stringList")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("longList")));
		assertEquals(IndexType.NUMERIC, IndexingService.getIndexType(getClass().getField("longCollection")));

		try {
			IndexingService.getIndexType(getClass().getField("charField"));
			assertFalse("This should not happen!", true);
		}
		catch (SpikeifyError e) {
			assertEquals("Can't index field: 'charField', indexing field type: 'char' not supported!", e.getMessage());
		}
	}

	@Test(expected = SpikeifyError.class)
	public void testIndexClash() {

		IndexingService.createIndex(sfy, new Policy(), EntityIndexed.class);
		try {
			IndexingService.createIndex(sfy, new Policy(), EntityIndexed2.class);
		}
		catch (SpikeifyError e) {
			assertEquals("Index: 'index_number' is already indexing entity: 'EntityIndexed', can not bind to: 'com.spikeify.entity.EntityIndexed2'", e.getMessage());
			throw e;
		}

	}

	@Test(expected = SpikeifyError.class)
	public void testIndexClash_2() {

		IndexingService.createIndex(sfy, new Policy(), EntityIndexed.class);

		try {
			IndexingService.createIndex(sfy, new Policy(), EntityIndexed3.class);
		}
		catch (SpikeifyError e) {
			assertEquals(
				"Index: 'idx_EntityIndexed_text' is already indexing field: 'text' on: 'EntityIndexed', remove this index before applying: 'failed_index' on: 'com.spikeify.entity.EntityIndexed3'!",
				e.getMessage());
			throw e;
		}

	}
}