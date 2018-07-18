package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.util.HashMap;
import java.util.Map;

public class SharedSpaceManager {

	private final long maxSize;

	private long used;

	private final Map<Long, Long> users;

	public SharedSpaceManager(long maxSize) {
		this.maxSize = maxSize;

		users = new HashMap<>();
	}

	public boolean request(long userId, long amount) {
		long available = maxSize - used;
		if (available < 0) {
			throw new IllegalStateException("Too many resources in use");
		}
		if (available < amount) {
			return false;
		}
		used += amount;

		Long userUsed = users.get(userId);
		if (userUsed == null) {
			userUsed = 0L;
		}
		userUsed += amount;
		users.put(userId, userUsed);

		return true;
	}

	public void release(long userId, long amount) {
		used -= amount;
		if (used < 0) {
			throw new IllegalStateException("Too many resources released");
		}

		Long userUsed = users.get(userId);
		if (userUsed == null) {
			throw new IllegalArgumentException("User " + userId + " has no allocated space to release");
		}
		userUsed -= amount;
		users.put(userId, userUsed);
	}

	public void releaseAll(long userId) {
		Long userUsed = users.remove(userId);
		if (userUsed == null) {
			throw new IllegalArgumentException("User " + userId + " has no allocated space to release");
		}
		used -= userUsed;
//		System.out.println("Used after releaseAll: " + used);
	}

}
