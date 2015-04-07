package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LoaderTest {

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig("localhost", 3000);
	}

	@Test
	public void loadProperties() {

		int one = 123;
		String two = "a test";
		double three = 123.0d;
		float four = 123.0f;
		short five = (short) 234;
		byte six = (byte) 100;

		Bin binOne = new Bin("one", one);
		Bin binTwo = new Bin("two", two);
		Bin binThree = new Bin("three", three);
		Bin binFour = new Bin("four", four);
		Bin binFive = new Bin("five", five);
		Bin binSix = new Bin("six", six);

		AerospikeClient client = new AerospikeClient("localhost", 3000);
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;

		String namespace = "test";
		String setName = "testSet";
		String userKey = "123456789";

		Key key = new Key(namespace, setName, userKey);
		client.put(policy, key, binOne, binTwo, binThree, binFour, binFive, binSix);

		EntityOne entity = SpikeifyService.sfy().load(EntityOne.class).key(userKey).namespace(namespace).set(setName).now();

		Assert.assertEquals(one, entity.one);
		Assert.assertEquals(two, entity.two);
		Assert.assertEquals(three, entity.three, 0.1);
		Assert.assertEquals(four, entity.four, 0.1);
		Assert.assertEquals(five, entity.getFive());
		Assert.assertEquals(six, entity.getSix());
	}

}
