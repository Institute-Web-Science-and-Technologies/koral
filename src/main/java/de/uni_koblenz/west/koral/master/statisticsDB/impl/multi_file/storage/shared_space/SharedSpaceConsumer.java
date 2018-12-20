package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space;

public interface SharedSpaceConsumer {

	/**
	 * Is called to request freeing of claimed space.
	 *
	 * @return True if space was freed, false if no space can be freed.
	 */
	public abstract boolean makeRoom();

	/**
	 * In general: Abstract costs for accessing this consumers data. Higher costs
	 * mean higher shares for the shared cache. The SharedSpaceManager computes the
	 * share of the total costs for each consumer to get one of its priority metrics
	 * for this consumer.
	 *
	 * In practice, this currently represents a value proportional to the file size
	 * of the extra files (RLE list max ID), because larger files require more hard
	 * disk seeking times.
	 *
	 * @return
	 */
	public abstract long accessCosts();

	/**
	 * Whether this consumer can make room if itself desires new space. Returning
	 * false will cause some SharedSpaceManager implementations to not ask this file
	 * to make room for itself.
	 *
	 * @return True if the consumer can make room even if the request was done by
	 *         itself.
	 */
	public abstract boolean isAbleToMakeRoomForOwnRequests();

}
