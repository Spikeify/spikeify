package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.mock.AerospikeClientMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

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
		Assert.assertEquals(reloaded.nine.size(), 1);
		Assert.assertTrue(reloaded.nine.contains("three"));
	}

	@Test
	public void testListUpdate() {

		client = new AerospikeClient("localhost", 3000);

		List aList = new ArrayList();
		aList.add("test1");
		aList.add("test2");
		aList.add(1234);
		aList.add(123.0d);

//		Bin bin1 = new Bin("one", aList);
		Bin bin2 = new Bin("two", 1.1f);
//		Bin bin3 = new Bin("three", false);

		Key saveKey = new Key(namespace, setName, userKey);

		// delete entity by hand
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		client.put(policy, saveKey, bin2);

		Record result = client.get(policy, saveKey);

		System.out.println();
	}

}
