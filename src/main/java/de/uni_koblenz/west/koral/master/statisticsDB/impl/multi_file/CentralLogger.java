package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

/**
 * General-purpose singleton class for all kinds of meta-logging. May be used to collect metrics or statistics of
 * different implementation parts without much effort for development/experimenting. Is not supposed to be used in
 * production settings.
 *
 * @author Philipp Töws
 *
 */
public class CentralLogger {

	private static CentralLogger instance;

	private long releaseExecutes;

	private long releaseFindBlockIterations;

	private CentralLogger() {}

	public static CentralLogger getInstance() {
		if (instance == null) {
			instance = new CentralLogger();
		}
		return instance;
	}

	public void addReleaseExecute() {
		releaseExecutes++;
	}

	public void addReleaseFindBlockIteration() {
		releaseFindBlockIterations++;
	}

	public void finish() {
		System.out.println("===== CentralLogger:");
		System.out.println("Release executes: " + String.format("%,d", releaseExecutes));
		System.out.println("Release FindBlock Iterations: " + String.format("%,d", releaseFindBlockIterations));
		System.out.println("===== End CentralLogger");
	}

}
