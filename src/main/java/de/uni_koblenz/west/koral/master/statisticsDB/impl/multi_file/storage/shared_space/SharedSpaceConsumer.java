package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space;

public interface SharedSpaceConsumer {

	public abstract boolean makeRoom();

	/**
	 * Whether this consumer can make room if itself desires new space. Returning false will cause some
	 * SharedSpaceManager implementations to not ask this file to make room for itself.
	 *
	 * @return True if the consumer can make room even if the request was done by itself.
	 */
	public abstract boolean isAbleToMakeRoomForOwnRequests();

}
