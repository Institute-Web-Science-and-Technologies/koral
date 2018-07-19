package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.util.HashMap;
import java.util.Map;

public class SharedSpaceManager {

	private final long maxSize;

	private long used;

	private final Map<SharedSpaceUser, Long> users;

	public SharedSpaceManager(long maxSize) {
		this.maxSize = maxSize;

		users = new HashMap<>();
	}

	public boolean isAvailable(long amount) {
		return amount < (maxSize - used);
	}

	public boolean request(SharedSpaceUser user, long amount) {
		long available = maxSize - used;
		if (available < 0) {
			throw new IllegalStateException("Too many resources in use");
		}
		if (available < amount) {
			return false;
		}
		used += amount;

		Long userUsed = users.get(user);
		if (userUsed == null) {
			userUsed = 0L;
		}
		userUsed += amount;
		users.put(user, userUsed);

		return true;
	}

	public void release(SharedSpaceUser user, long amount) {
		used -= amount;
		if (used < 0) {
			throw new IllegalStateException("Too many resources released");
		}

		Long userUsed = users.get(user);
		if (userUsed == null) {
			throw new IllegalArgumentException("User " + user + " has no allocated space to release");
		}
		userUsed -= amount;
		users.put(user, userUsed);
	}

	public void releaseAll(SharedSpaceUser user) {
		Long userUsed = users.remove(user);
		if (userUsed == null) {
			throw new IllegalArgumentException("User " + user + " has no allocated space to release");
		}
		used -= userUsed;
//		System.out.println("Used after releaseAll: " + used);
	}

}
