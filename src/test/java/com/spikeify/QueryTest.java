package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;
import com.spikeify.mock.AerospikeClientMock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class QueryTest {

	private String namespace = "test";
	private String setName = "testSetQuery";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig("localhost", 3000);
		client = new AerospikeClientMock();
		sfy = SpikeifyService.mock(client);
	}

	@Test
	public void testQueryList() {

		AerospikeClient client = new AerospikeClient("localhost", 3000);
		client.createIndex(new Policy(), namespace, setName, setName + "_index", "one", IndexType.STRING, IndexCollectionType.LIST);
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;

		// first record
		List list1 = new ArrayList();
		list1.add("one");
		list1.add("two");
		list1.add("three");
		Bin bin1 = new Bin("one", list1);
		Long userKey1 = new Random().nextLong();
		Key key1 = new Key(namespace, setName, userKey1);
		client.put(policy, key1, bin1);

		// second record
		List list2 = new ArrayList();
		list2.add("three");
		list2.add("four");
		list2.add("five");
		Bin bin2 = new Bin("one", list2);
		Long userKey2 = new Random().nextLong();
		Key key2 = new Key(namespace, setName, userKey2);
		client.put(policy, key2, bin2);

		Statement query = new Statement();
		query.setNamespace(namespace);
		query.setSetName(setName);
		query.setIndexName(setName + "_index");
		query.setBinNames("one");
		Filter filter = Filter.contains("one", IndexCollectionType.LIST, "three");
		query.setFilters(filter);

		RecordSet result = client.query(new QueryPolicy(), query);

		while (result.next()) {
			Key key = result.getKey();
			Record record = result.getRecord();

			System.out.println(key.toString());
			for (Map.Entry<String,Object> bin : record.bins.entrySet()) {
				System.out.println("   "+bin.getKey()+":"+bin.getValue());

			}
		}

	}

	@Test
	public void testQuery() {

		AerospikeClient client = new AerospikeClient("localhost", 3000);
//		client.createIndex(new Policy(), namespace, setName, setName + "_index2", "one", IndexType.STRING, IndexCollectionType.MAPKEYS);
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;

		// first record

		Bin bin1 = new Bin("one", "456");
		Bin bin2 = new Bin("two", "bbb");
		Long userKey1 = new Random().nextLong();
		Key key1 = new Key(namespace, setName, userKey1);
		client.put(policy, key1, bin1, bin2);

		Statement query = new Statement();
		query.setNamespace(namespace);
		query.setSetName(setName);
		query.setIndexName(setName + "_index2");
		query.setBinNames("one");
		Filter filter = Filter.contains("one", IndexCollectionType.LIST, "123");
		query.setFilters(filter);
//
//		RecordSet result = client.query(new QueryPolicy(), query);
//
//		while (result.next()) {
//			Key key = result.getKey();
//			Record record = result.getRecord();
//
//			System.out.println(key.toString());
//			for (Map.Entry<String, Object> bin : record.bins.entrySet()) {
//				System.out.println("   " + bin.getKey() + ":" + bin.getValue());
//
//			}
//		}
	}

}
