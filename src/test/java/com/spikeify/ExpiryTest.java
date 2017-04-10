package com.spikeify;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.entity.EntityExpires;
import com.spikeify.entity.EntityTtl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

@SuppressWarnings({"unchecked", "UnusedAssignment"})
public class ExpiryTest extends SpikeifyTest {

	private final Long userKey1 = new Random().nextLong();
	private final Long userKey2 = new Random().nextLong();
	private final String setName = "newTestSet";

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

		if (defaultTTLmsec == 0) {  // namespace is set to never expire
			Assert.assertEquals(reloaded.expires.longValue(), -1L);
		} else {
			long now = new Date().getTime();
			Assert.assertTrue(now > reloaded.expires - defaultTTLmsec);
			Assert.assertTrue(reloaded.expires > now);
		}

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
		long futureDate = System.currentTimeMillis() + (5L * milliSecYear);
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

	@Test
	public void setExpiresLowLevel() {

		Bin binTest = new Bin("one", 111);

		int expirationSec = 100;

		Key key = new Key(namespace, setName, userKey1);
		WritePolicy wp = new WritePolicy();
		wp.expiration = expirationSec;
		sfy.getClient().put(wp, key, binTest);

		int absExp = sfy.getClient().get(null, key).expiration;
		int relExp = ExpirationUtils.getExpiresRelative(absExp);

		Assert.assertTrue(expirationSec - relExp < 5);
	}

	/**
	 * Fixed with TimeToLive
	 *
	 * Set relative expiration date problem. Test is successful, but record will never expire.
	 * <p>
	 * expire value is mapped correctly by Spikeify as 86400000.
	 * But Spikeify saves the expire value in AS as -1453219341 and AS logs following warning:
	 * WARNING (rw): (thr_rw.c::3136) {test} ttl 2841748150 exceeds 315360000 - set config value max-ttl to suppress this warning <Digest>:0x5da19e0a4e90067b2eada5a63afbfd21a71c44b4
	 * So record will never expire.
	 */
	@Test
	public void setExpiresRelative() {
		EntityTtl entity = new EntityTtl();

		long relativeDate = 24L * 60L * 60L; //expire in one day
		entity.ttl = relativeDate;
		Key key1 = new Key(namespace, setName, userKey1);

		Key saveKey = sfy
				.update(key1, entity)
				.now();

		EntityTtl reloaded = sfy.get(EntityTtl.class).key(saveKey).now();
		Assert.assertEquals(relativeDate, reloaded.ttl, 5); //test succeeds
	}

	/**
	 * sfy.command automatically sets expire to -1, which is wrong.
	 * The setExpires (relatve) has to be called explicitly
	 */
	@Test
	public void setExpiresCommandRetrieveFlow() {
		EntityExpires entity = new EntityExpires();

		long milliSecDay = 24L * 60L * 60L * 1000L;
		long milliSecYear = 100L * milliSecDay;
		long futureDate = new Date().getTime() + 1L * milliSecYear;
		entity.expires = futureDate;
		final Key key1 = new Key(namespace, setName, userKey1);

		Key saveKey = sfy
				.update(key1, entity)
				.now();

		sfy.command(EntityExpires.class).key(key1).add("one", 1).now(); //Error: it sets expire to -1

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();
		Assert.assertEquals(-1, reloaded.expires.longValue());

		sfy.command(EntityExpires.class).setExpires(futureDate).key(key1).add("one", 1).now(); //Error: it sets expire to -1
		reloaded = sfy.get(EntityExpires.class).key(saveKey).now();

		Assert.assertEquals(entity.expires, reloaded.expires, 1);
		Assert.assertEquals(futureDate, reloaded.expires, 1);
	}

	@Test
	public void setTtlCommandRetrieveFlow() {
		EntityTtl entity = new EntityTtl();

		long secDay = 24L * 60L * 60L;
		long secYear = 365L * secDay;
		entity.ttl = 5L * secYear;
		final Key key1 = new Key(namespace, setName, userKey1);

		Key saveKey = sfy
				.update(key1, entity)
				.now();

		sfy.command(EntityTtl.class).setTtl(entity.ttl).key(key1).add("one", 1).touch().now(); //Error: it sets expire to -1

		EntityTtl reloaded = sfy.get(EntityTtl.class).key(saveKey).now();

		Assert.assertEquals(entity.ttl, reloaded.ttl, 1);
	}

	/**
	 * sfy.update does change expire time, which is correct.
	 */
	@Test
	public void setExpiresUpdateRetrieveFlow() {
		EntityExpires entity = new EntityExpires();

		long milliSecDay = 24L * 60L * 60L * 1000L;
		long milliSecYear = 365L * milliSecDay;
		long futureDate = new Date().getTime() + 5L * milliSecYear;
		entity.expires = futureDate;
		final Key key1 = new Key(namespace, setName, userKey1);

		final Key saveKey = sfy
				.update(key1, entity)
				.now();

		//update one field
		for (int i = 0; i < 100; i++) {
			sfy.transact(5, new Work<EntityExpires>() {
				@Override
				public EntityExpires run() {
					EntityExpires obj = sfy.get(EntityExpires.class).key(key1).now();
					if (obj != null) {
						obj.one++;
						sfy.update(obj).now();
					}
					return obj;
				}
			});
		}

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();

		System.out.println("original: " + new Date(entity.expires.longValue()));
		System.out.println("reloaded: " + new Date(reloaded.expires.longValue()));
		System.out.println("future: " + new Date(futureDate));

		System.out.println("diff: " + (entity.expires.longValue() - reloaded.expires.longValue()));

		Assert.assertEquals(reloaded.one, 100);
		Assert.assertEquals(futureDate, entity.expires.longValue());
		Assert.assertEquals(futureDate, reloaded.expires.longValue(), 500000);
		Assert.assertEquals(entity.expires, reloaded.expires, 500000);
	}

	@Test
	public void testExpiredUpdate() {

		EntityExpires entity = new EntityExpires();

		long futureDate = System.currentTimeMillis() + 2000l;
		entity.expires = futureDate;
		final Key key1 = new Key(namespace, setName, userKey1);

		final Key saveKey = sfy
				.create(key1, entity)
				.now();

		EntityExpires reloaded = sfy.get(EntityExpires.class).key(saveKey).now();

		Assert.assertEquals(entity.expires, reloaded.expires, 500000);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		reloaded.expires = System.currentTimeMillis() + 120000l;
		Key updateKey = sfy
				.update(key1, reloaded)
				.now();

		reloaded = sfy.get(EntityExpires.class).key(updateKey).now();
		Assert.assertEquals(entity.expires, reloaded.expires, 500000);
	}

	@Test
	public void testTtlExpirationUpdate() {

		EntityTtl entity = new EntityTtl();
		entity.ttl = 1l;
		final Key key1 = new Key(namespace, setName, userKey1);

		final Key saveKey = sfy
				.create(key1, entity)
				.now();

		EntityTtl reloaded = sfy.get(EntityTtl.class).key(saveKey).now();
		Assert.assertEquals(entity.ttl, reloaded.ttl, 1);

		// wait to expire...
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// expired... should create new one
		reloaded.ttl = 1l;
		Key updateKey = sfy
				.update(key1, reloaded)
				.now();

		reloaded = sfy.get(EntityTtl.class).key(updateKey).now();
		Assert.assertEquals(entity.ttl, reloaded.ttl, 1);

	}

	@Test
	public void testTtlExpirationMultiUpdate() {

		EntityTtl entity1 = new EntityTtl();
		entity1.userId = 1l;
		entity1.ttl = 2l;
		entity1.one = 1;
		EntityTtl entity2 = new EntityTtl();
		entity2.userId = 2l;
		entity2.ttl = 2l;
		entity2.one = 2;

		final Map<Key, Object> savedMap = sfy
				.createAll(entity1, entity2)
				.now();

		Assert.assertEquals(2, savedMap.size());
		Map<Key, EntityTtl> reloaded = sfy.getAll(EntityTtl.class, savedMap.keySet().toArray(new Key[savedMap.size()])).now();
		Iterator<EntityTtl> it = reloaded.values().iterator();
		EntityTtl res1 = it.next();
		EntityTtl res2 = it.next();
		Assert.assertEquals(1, res1.one);
		Assert.assertEquals(2, res2.one);
		Assert.assertEquals(1, res1.ttl.intValue(), 1);
		Assert.assertEquals(1, res2.ttl.intValue(), 1);

		// wait to expire...
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// expired... should create new one
		Map<Key, Object> updateMap = sfy
				.updateAll(entity1, entity2)
				.now();

		reloaded = sfy.getAll(EntityTtl.class, updateMap.keySet().toArray(new Key[updateMap.size()])).now();
		Iterator<Object> itr = updateMap.values().iterator();
		Assert.assertEquals(2, updateMap.size());
		EntityTtl rel1 = (EntityTtl)itr.next();
		EntityTtl rel2 = (EntityTtl)itr.next();
		Assert.assertEquals(1, rel1.one);
		Assert.assertEquals(2, rel2.one);

		Assert.assertEquals(1, rel1.ttl.intValue(), 1);
		Assert.assertEquals(1, rel2.ttl.intValue(), 1);

	}

}
