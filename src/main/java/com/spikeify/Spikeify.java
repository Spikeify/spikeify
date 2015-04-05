package com.spikeify;

public interface Spikeify {

	Loader load();

	Updater insert();

	Updater update();

	Deleter delete();

	<R> R transact(Work<R> work);
}
