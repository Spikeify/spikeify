package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.mock.AerospikeClientMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class UpdaterTest {

	private Long userKey = new Random().nextLong();
	private String namespace = "test";
	private String setName = "testSet";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig("localhost", 3000);
		client = new AerospikeClientMock();
		sfy = SpikeifyService.mock(client);
	}

	@After
	public void dbCleanup() {
		Key deleteKey = new Key(namespace, setName, userKey);
		sfy.delete().key(deleteKey).now();
	}

	@Test
	public void saveProperties() {

		EntityOne entity = new EntityOne();

		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;
		entity.eight = new Date(1420070400);

		Key saveKey = sfy
				.update(entity)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		Key loadKey = new Key(namespace, setName, userKey);

		Policy policy = new Policy();
		policy.sendKey = true;
		Record record = client.get(policy, loadKey);

		Assert.assertEquals(entity.one, record.getInt("one"));
		Assert.assertEquals(entity.two, record.getString("two"));
		Assert.assertEquals(entity.three, record.getDouble("three"), 0.1);
		Assert.assertEquals(entity.four, record.getFloat("four"), 0.1);
		Assert.assertEquals(entity.getFive(), record.getShort("five"));
		Assert.assertEquals(entity.getSix(), record.getByte("six"));
		Assert.assertEquals(entity.seven, record.getBoolean("seven"));
		Assert.assertEquals(entity.eight, new Date(record.getLong("eight")));

	}

	@Test
	public void testDifferentialUpdate() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;
		entity.eight = new Date(1420070400);
		entity.nine = new ArrayList<>();
		entity.nine.add("one");
		entity.nine.add("two");

		Key saveKey = sfy
				.update(entity)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		// delete entity by hand
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		boolean deleted = client.delete(policy, saveKey);
		Assert.assertTrue(deleted); // was indeed deleted

		// change two properties
		entity.one = 100;
		entity.two = "new string";
		entity.nine.add("three");

		sfy.update(entity)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		// reload entity and check that only two properties were updated
		EntityOne reloaded = sfy.load(EntityOne.class)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		Assert.assertEquals(reloaded.one, 100);
		Assert.assertEquals(reloaded.two, "new string");
		Assert.assertEquals(reloaded.three, 0, 0.1);
		Assert.assertEquals(reloaded.four, 0, 0.1);
		Assert.assertEquals(reloaded.getFive(), 0);
		Assert.assertEquals(reloaded.getSix(), 0);
		Assert.assertEquals(reloaded.eight, null);
		Assert.assertEquals(reloaded.nine.size(), 3);
		Assert.assertTrue(reloaded.nine.contains("three"));
	}

	@Test
	public void testListUpdate() {

		List aList = new ArrayList();
		aList.add("test1");
		aList.add("test2");
		aList.add(1234);
		aList.add(123.0d);

		Bin bin1 = new Bin("one", aList);
		Bin bin2 = new Bin("two", 1.1f);
		Bin bin3 = new Bin("three", false);

		Key saveKey = new Key(namespace, setName, userKey);

		// save entity manually
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		client.put(policy, saveKey, bin1, bin2, bin3);

		Record result = client.get(policy, saveKey);

		Assert.assertNotNull(result.bins.get("one"));
		Assert.assertEquals(4, ((List) result.bins.get("one")).size());
	}

	@Test
	public void testMapUpdate() {

		Map aMap = new HashMap();
		aMap.put("1", "test1");
		aMap.put("2", "test2");
		aMap.put("3", 1234);
		aMap.put("4", 123.0d);

		Bin bin1 = new Bin("one", aMap);
		Bin bin2 = new Bin("two", 1.1f);
		Bin bin3 = new Bin("three", false);

		Key saveKey = new Key(namespace, setName, userKey);

		// save entity manually
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		client.put(policy, saveKey, bin1, bin2, bin3);

		Record result = client.get(policy, saveKey);

		Assert.assertNotNull(result.bins.get("one"));
		Assert.assertEquals(4, ((Map) result.bins.get("one")).size());
	}

	@Test
	public void entityListMapUpdate() {

		List aList = new ArrayList();
		aList.add("test1");
		aList.add("test2");
		aList.add(1234);
		aList.add(123.0d);

		Map aMap = new HashMap();
		aMap.put("1", "testX");
		aMap.put("2", "testY");
		aMap.put("3", 456);
		aMap.put("4", 456.0d);


		EntityOne entityOne = new EntityOne();
		entityOne.nine = aList;
		entityOne.ten = aMap;

		Key saveKey = new Key(namespace, setName, userKey);

		// save entity
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		Key savedKey = sfy.update(entityOne).key(saveKey).now();

		// load entity
		EntityOne loadedEntity = sfy.load(EntityOne.class).key(savedKey).now();

		// check values
		List nine = loadedEntity.nine;
		Map ten = loadedEntity.ten;
		Assert.assertEquals(4, nine.size());
		Assert.assertEquals(aList, nine);
		Assert.assertEquals(4, ten.size());
		Assert.assertEquals(aMap, ten);
	}

}
