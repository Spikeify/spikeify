package com.spikeify;

import com.spikeify.entity.EntityAutoKey;
import com.spikeify.entity.EntityAutoKey2;
import com.spikeify.entity.EntityAutoKeyToFail;
import com.spikeify.entity.EntityOne;
import org.junit.Test;

import static org.junit.Assert.*;

public class IdGeneratorTest {

	@Test
	public void shouldGenerateIdTest() {

		EntityOne one = new EntityOne();
		assertFalse(IdGenerator.shouldGenerateId(one));

		EntityAutoKey auto = new EntityAutoKey();
		assertTrue(IdGenerator.shouldGenerateId(auto));
	}

	@Test
	public void generateId() {

		// 0
		EntityAutoKey auto = new EntityAutoKey();
		IdGenerator.generateId(auto);

		assertNotNull(auto.key);
		assertEquals(10, auto.key.length());

		// 1
		EntityAutoKey2 auto2 = new EntityAutoKey2();
		IdGenerator.generateId(auto2);

		assertNotNull(auto2.key);
		assertEquals(2, auto2.key.toString().length());

		// 2
		EntityAutoKeyToFail auto3 = new EntityAutoKeyToFail();
		IdGenerator.generateId(auto3);

		assertNotNull(auto3.key);
		assertEquals(1, auto3.key);
	}
}