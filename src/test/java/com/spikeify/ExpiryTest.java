package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.spikeify.entity.EntityExpires;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.Random;

@SuppressWarnings({"unchecked", "UnusedAssignment"})
public class ExpiryTest {

	private final Long userKey1 = new Random().nextLong();
	private final Long userKey2 = new Random().nextLong();
	private final String namespace = "test";
	private final String setName = "newTestSet";
	private Spikeify sfy;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		IAerospikeClient client = SpikeifyService.getClient();
		sfy = SpikeifyService.sfy();
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void defaultDbExpires() {
		EntityExpires entity = new EntityExpires();
		entity.expires = 0l;
		Key key1 = new Key(namespace, setName, userKey1);

		Key saveKey = sfy
				.update(key1, entity)
				.now();

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		long milliSecDay = 24 * 60 * 60 * 1000;
		long now = new Date().getTime();
		Assert.assertTrue(now + 5 * milliSecDay >= reloaded.expires);
		Assert.assertTrue(reloaded.expires> now);
	}

	@Test
	public void doesNotExpire() {
		EntityExpires entity = new EntityExpires();
		entity.expires = -1l;
		Key key1 = new Key(namespace, setName, userKey1);

		// we did not provide namespace on purpose - let default kick in
		Key saveKey = sfy
				.update(key1, entity)
				.now();

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		Assert.assertEquals(entity.expires, reloaded.expires);
		Assert.assertEquals(-1l, reloaded.expires.longValue());
	}

	@Test
	public void setExpires() {
		EntityExpires entity = new EntityExpires();

		long milliSecDay = 24l * 60l * 60l * 1000l;
		long milliSecYear = 365l * milliSecDay;
		long futureDate = new Date().getTime() + 5l * milliSecYear;
		entity.expires = futureDate;
		Key key1 = new Key(namespace, setName, userKey1);

		Key saveKey = sfy
				.update(key1, entity)
				.now();

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		Assert.assertEquals(futureDate, entity.expires.longValue());
		Assert.assertEquals(entity.expires, reloaded.expires, 5000);
		Assert.assertEquals(futureDate, reloaded.expires.longValue(), 5000);
	}

}
