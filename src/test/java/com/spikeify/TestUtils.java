package com.spikeify;

import com.spikeify.entity.EntityOne;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class TestUtils {

	public static Random random = new Random();

	private static String[] wordBase = ("Lorem ipsum dolor sit amet consectetur adipiscing elit Integer nec odio Praesent libero " +
			"Sed cursus ante dapibus diam Sed nisi Nulla quis sem at nibh elementum imperdiet Duis sagittis ipsum Praesent mauris " +
			"Fusce nec tellus sed augue semper porta Mauris massa Vestibulum lacinia arcu eget nulla " +
			"Class aptent taciti sociosqu ad litora torquent per conubia nostra per inceptos himenaeos " +
			"Curabitur sodales ligula in libero Sed dignissim lacinia nunc Curabitur tortor Pellentesque nibh Aenean quam " +
			"In scelerisque sem at dolor Maecenas mattis Sed convallis tristique sem Proin ut ligula vel nunc egestas porttitor " +
			"Morbi lectus risus iaculis vel suscipit quis luctus non massa Fusce ac turpis quis ligula lacinia aliquet " +
			"Mauris ipsum Nulla metus metus ullamcorper vel tincidunt sed euismod in nibh Quisque volutpat condimentum velit ").split("\\s+");


	public static String randomWord() {
		return wordBase[random.nextInt(wordBase.length)];
	}


	public static List<EntityOne> randomEntityOne(int number, String setName) {
		List<EntityOne> res = new ArrayList<>(number);
		for (int i = 0; i < number; i++) {
			EntityOne ent = new EntityOne();
			ent.userId = new Random().nextLong();
			ent.theSetName = setName;
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

}
