package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.spikeify.mock.AerospikeClientMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class DeleterTest {

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

	@Test
	public void deleteRecord() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;

		Key saveKey = sfy
				.create(entity)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		Key deleteKey = new Key(namespace, setName, userKey);
		sfy.delete().key(deleteKey).now();

		Policy policy = new Policy();
		policy.sendKey = true;
		boolean exists = client.exists(null, saveKey);

		// assert record does not exist
		Assert.assertFalse(exists);
	}

}
