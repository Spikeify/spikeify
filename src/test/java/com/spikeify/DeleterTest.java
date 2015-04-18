package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.spikeify.entity.EntityOne;
import com.spikeify.mock.AerospikeClientMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class DeleterTest {

	private Random random = new Random();
	private Long userKeyLong = random.nextLong();
	private String userKeyString = String.valueOf(random.nextLong());
	private String namespace = "test";
	private String setName = "testSet";


	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig("localhost", 3000, "test");
		client = new AerospikeClientMock();
		sfy = SpikeifyService.mock(client);
	}

	private List<EntityOne> createEntityOne(int number) {
		List<EntityOne> res = new ArrayList<>(number);
		for (int i = 0; i < number; i++) {
			EntityOne ent = new EntityOne();
			ent.userId = new Random().nextLong();
			ent.one = random.nextInt();
			ent.two = TestUtils.randomWord();
			ent.three = random.nextDouble();
			ent.four = random.nextFloat();
			ent.setFive((short) random.nextInt());
			ent.setSix((byte) random.nextInt());
			ent.seven = random.nextBoolean();
			ent.eight = new Date(random.nextLong());
			res.add(ent);
		}
		return res;
	}

	@Test
	public void deleteRecordViaKey() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;

		// we did not provide namespace on purpose - let default kick in
		Key saveKey = sfy
				.create(userKeyLong, entity)
				.set(setName)
				.now();

		Key deleteKey = new Key(namespace, setName, userKeyLong);
		sfy.delete(deleteKey).now();

		Policy policy = new Policy();
		policy.sendKey = true;
		boolean exists = client.exists(null, saveKey);

		// assert record does not exist
		Assert.assertFalse(exists);
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
				.create(userKeyString, entity)
				.namespace(namespace)
				.set(setName)
				.now();

		sfy.delete(userKeyString).namespace(namespace).set(setName).now();

		Policy policy = new Policy();
		policy.sendKey = true;
		boolean exists = client.exists(null, saveKey);

		// assert record does not exist
		Assert.assertFalse(exists);
	}

	@Test
	public void deleteObject() {

		EntityOne entity = createEntityOne(1).get(0);

		Key saveKey = sfy
				.create(entity)
				.now();

		boolean deleted = sfy.delete(entity).now();
		Assert.assertTrue(deleted);

		Policy policy = new Policy();
		policy.sendKey = true;
		boolean exists = client.exists(null, saveKey);

		// assert record does not exist
		Assert.assertFalse(exists);
	}

}
