package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class UpdaterTest {

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig("localhost", 3000);
	}

	@Test
	public void saveProperties() {

		String namespace = "test";
		String setName = "testSet";
		String userKey = "123456789";

		EntityOne entity = new EntityOne();

		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);

		Key saveKey = SpikeifyService.sfy()
				.update(entity)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		Key loadKey = new Key(namespace, setName, userKey);

		AerospikeClient client = new AerospikeClient("localhost", 3000);
		Policy policy = new Policy();
		policy.sendKey = true;
		Record record = client.get(policy, loadKey);

		Assert.assertEquals(entity.one, record.getInt("one"));
		Assert.assertEquals(entity.two, record.getString("two"));
		Assert.assertEquals(entity.three, record.getDouble("three"), 0.1);
		Assert.assertEquals(entity.four, record.getFloat("four"), 0.1);
		Assert.assertEquals(entity.getFive(), record.getShort("five"));
		Assert.assertEquals(entity.getSix(), record.getByte("six"));
	}

}
