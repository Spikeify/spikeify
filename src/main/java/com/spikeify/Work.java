package com.spikeify;


/**
 * A unit of work that wants to be transactionally guarded.
 * In case of transactional failure, work will be repeated, so it must be idempotent.
 * @param <R>
 */
public interface Work<R> {

	R run();

}
