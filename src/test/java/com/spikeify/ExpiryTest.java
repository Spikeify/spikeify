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

	/**
	 * Set relative expiration date problem. Test is successful, but record will never expire.
	 *
	 * expire value is mapped correctly by Spikeify as 86400000.
	 * But Spikeify saves the expire value in AS as -1453219341 and AS logs following warning:
	 * WARNING (rw): (thr_rw.c::3136) {test} ttl 2841748150 exceeds 315360000 - set config value max-ttl to suppress this warning <Digest>:0x5da19e0a4e90067b2eada5a63afbfd21a71c44b4
	 * So record will never expire.
	 *
	 */
	@Test
	public void setExpiresRelative() {
		EntityExpires entity = new EntityExpires();

		long relativeDate = 24L * 60L * 60L * 1000L; //expire in one day
		entity.expires = relativeDate;
		Key key1 = new Key(namespace, setName, userKey1);

		Key saveKey = sfy
				.update(key1, entity)
				.now();

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		Assert.assertEquals(relativeDate, reloaded.expires, 5000); //test succeeds
	}

	/**
	 * sfy.command automatically sets expire to -1, which is wrong.
	 */
	@Test
	public void setExpiresCommandRetrieveFlow() {
		EntityExpires entity = new EntityExpires();

		long milliSecDay = 24L * 60L * 60L * 1000L;
		long milliSecYear = 365L * milliSecDay;
		long futureDate = new Date().getTime() + 5L * milliSecYear;
		entity.expires = futureDate;
		final Key key1 = new Key(namespace, setName, userKey1);

		Key saveKey = sfy
				.update(key1, entity)
				.now();

		sfy.command(EntityExpires.class).key(key1).add("one", 1).now(); //Error: it sets expire to -1

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		Assert.assertEquals(futureDate, entity.expires.longValue());
		Assert.assertEquals(entity.expires, reloaded.expires, 5000);
		Assert.assertEquals(futureDate, reloaded.expires, 5000);
	}

	/**
	 * sfy.update does change expire time, which is correct.
	 */
	@Test
	public void setExpiresUpdateRetrieveFlow() {
		EntityExpires entity = new EntityExpires();

		long futureDate = new Date().getTime() + 10L * 1000L;
		entity.expires = futureDate;
		final Key key1 = new Key(namespace, setName, userKey1);

		final Key saveKey = sfy
				.update(key1, entity)
				.now();

		//update one field
		sfy.transact(5, new Work<EntityExpires>() {
			@Override
			public EntityExpires run() {
				EntityExpires obj = sfy.get(EntityExpires.class).key(key1).now();
				if(obj != null){
					obj.one++;
					sfy.update(obj).now();
				}
				return obj;
			}
		});

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		Assert.assertEquals(reloaded.one, 1);
		Assert.assertEquals(futureDate, entity.expires.longValue());
		Assert.assertEquals(entity.expires, reloaded.expires, 5000);
		Assert.assertEquals(futureDate, reloaded.expires, 5000);
	}



}
