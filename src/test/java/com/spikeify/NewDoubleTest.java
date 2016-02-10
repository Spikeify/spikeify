package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.entity.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * This test tests the new double support introduced in Aerospike Client 3.1.4
 */

@SuppressWarnings("ConstantConditions")
public class NewDoubleTest extends SpikeifyTest {

	private final Long userKey1 = new Random().nextLong();
	private final Long userKey2 = new Random().nextLong();
	private final String setName = "testSet";

	@Test
	public void saveDoubleLoadAsDouble() {

		EntityOne entityOne = TestUtils.randomEntityOne(setName);

		entityOne.three = 123.0d;
		entityOne.four = 123.0f;

		Bin binThree = new Bin("third", entityOne.three); // explicitly set bin name via @BinName annotation
		Bin binFour = new Bin("four", entityOne.four); // explicitly set bin name via @BinName annotation

		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;

		Key key = new Key(namespace, setName, userKey1);
		client.put(policy, key, binThree, binFour);

		Record reloaded = client.get(null, key);

		// testing default namespace - we did not explicitly provide namespace
		EntityOne entity = sfy.get(EntityOne.class).key(userKey1).namespace(namespace).setName(setName).now();

		// UserKey value
		assertEquals(userKey1, entity.userId);

		// field values
		assertEquals(entityOne.three, entity.three, 0.1);
		assertEquals(entityOne.four, entity.four, 0.1);
	}


}
