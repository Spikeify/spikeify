package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.spikeify.commands.InfoFetcher;
import com.spikeify.entity.EntityIndexed;
import com.spikeify.entity.EntityIndexed2;
import com.spikeify.entity.EntityOne;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class IndexingServiceTest {

	private final String namespace = "test";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		client = SpikeifyService.getClient();
		sfy = SpikeifyService.sfy();
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void testCreateIndex() throws Exception {

		// create index ...
		IndexingService.createIndex(sfy, new Policy(), EntityOne.class);
		IndexingService.createIndex(sfy, new Policy(), EntityIndexed.class);

		// check if index exists ...
		Map<String, InfoFetcher.IndexInfo> indexes = sfy.info().getIndexes(namespace, EntityOne.class);
		assertEquals(2, indexes.size());

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

		// 2.
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

		// 3.
		indexes = sfy.info().getIndexes(namespace, EntityIndexed.class);
		assertEquals(5, indexes.size());

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
	}

	@Test(expected = SpikeifyError.class)
	public void testIndexClash() {

		IndexingService.createIndex(sfy, new Policy(), EntityIndexed.class);
		try {
			IndexingService.createIndex(sfy, new Policy(), EntityIndexed2.class);
		}
		catch (SpikeifyError e) {
			assertEquals("Index: 'index_number' is already indexing entity: 'EntityIndexed', can not bind to: 'EntityIndexed2'", e.getMessage());
			throw e;
		}

	}
}