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

		long defaultTTLmsec = 1000 * sfy.info().getDefaultTTL(namespace);

		EntityExpires entity = new EntityExpires();
		entity.expires = 0L;
		Key key1 = new Key(namespace, setName, userKey1);

		Key saveKey = sfy
				.update(key1, entity)
				.now();

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		long now = new Date().getTime();
		Assert.assertTrue(now > reloaded.expires - defaultTTLmsec);
		Assert.assertTrue(reloaded.expires > now);
	}

	@Test
	public void doesNotExpire() {
		EntityExpires entity = new EntityExpires();
		entity.expires = -1L;
		Key key1 = new Key(namespace, setName, userKey1);

		// we did not provide namespace on purpose - let default kick in
		Key saveKey = sfy
				.update(key1, entity)
				.now();

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		Assert.assertEquals(entity.expires, reloaded.expires);
		Assert.assertEquals(-1L, reloaded.expires.longValue());
	}

	@Test
	public void setExpires() {
		EntityExpires entity = new EntityExpires();

		long milliSecDay = 24L * 60L * 60L * 1000L;
		long milliSecYear = 365L * milliSecDay;
		long futureDate = new Date().getTime() + 5L * milliSecYear;
		entity.expires = futureDate;
		Key key1 = new Key(namespace, setName, userKey1);

		Key saveKey = sfy
				.update(key1, entity)
				.now();

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		Assert.assertEquals(futureDate, entity.expires.longValue());
		Assert.assertEquals(entity.expires, reloaded.expires, 5000);
		Assert.assertEquals(futureDate, reloaded.expires, 5000);
	}

}
