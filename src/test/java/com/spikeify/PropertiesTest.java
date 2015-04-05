package com.spikeify;

import org.junit.Assert;
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

		Assert.assertEquals(123, props.get("one"));
		Assert.assertEquals("a test", props.get("two"));
		Assert.assertEquals(123.0d, props.get("three"));
		Assert.assertEquals(123.0f, props.get("four"));
		Assert.assertEquals((short) 234, props.get("five"));
		Assert.assertEquals((byte) 100, props.get("six"));
		Assert.assertFalse(props.containsKey("ignore"));
	}
}
