package com.spikeify;


/**
 * A unit of work that wants to be transactionally guarded.
 * In case of transactional failure, work will be repeated, so it must be idempotent.
 * @param <R> class type
 */
@SuppressWarnings({"SameReturnValue", "WeakerAccess"})
public interface Work<R> {

	R run();

}
