package com.spikeify;

import org.junit.Test;

import java.util.Map;

public class PropertiesTest {

	@Test
	public void loadProps(){

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.ignored = "should be ignored";

		ClassMapper mapper = new ClassMapper(EntityOne.class);
		Map<String, Object> props = mapper.getProperties(entity);

		System.out.println();
	}
}
