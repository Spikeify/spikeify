package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.spikeify.entity.EntityOne;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class QueryTest {

	private String namespace = "test";
	private String setName = "testSetQuery";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig("localhost", 3000, namespace);
		client = new AerospikeClient("localhost", 3000);
		sfy = SpikeifyService.sfy();
	}

	@After
	public void dbCleanup() {
		client.scanAll(null, namespace, setName, new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				client.delete(null, key);
			}
		});
	}

	@Test
	public void testEntityQuery() {

		client.createIndex(new Policy(), namespace, setName, setName + "_index", "two", IndexType.STRING);
		client.createIndex(new Policy(), namespace, setName, setName + "_index_long", "one", IndexType.NUMERIC);

		// create records
		for (int i = 0; i < 100; i++) {
			EntityOne ent = TestUtils.randomEntityOne(1).get(0);
			ent.theSetName = setName;
			if (i % 10 == 0) {
				ent.two = "content";
				ent.one = TestUtils.random.nextInt(1000);
			}
			sfy.create(ent).now();
		}

		EntitySet<EntityOne> entities = sfy.query(EntityOne.class)
				.indexName(setName + "_index")
				.setName(setName)
				.setFilters(Filter.equal("two", "content"))
				.now();

		while (entities.next()) {
			Assert.assertEquals("content", entities.getObject().two);
		}

		EntitySet<EntityOne> entities2 = sfy.query(EntityOne.class)
				.indexName(setName + "_index")
				.setName(setName)
				.setFilters(Filter.equal("two", "content"))
				.now();

		while (entities2.next()) {
			EntityOne entity = entities2.getObject();
			Assert.assertTrue(0 < entity.one && entity.one < 1000);
		}
	}

}
