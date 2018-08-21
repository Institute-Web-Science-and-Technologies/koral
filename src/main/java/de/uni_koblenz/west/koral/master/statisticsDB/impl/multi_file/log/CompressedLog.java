package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

public class CompressedLog {

	private LayoutEntry[] layout;

	public CompressedLog() {
		// TODO Auto-generated constructor stub
	}

	public enum LayoutType {
		BIT, BYTE, SHORT, INTEGER
	}

	class LayoutEntry {
		public final String key;
		public final LayoutType type;

		public LayoutEntry(String key, LayoutType type) {
			this.key = key;
			this.type = type;
		}
	}

	public void setRowLayout(LayoutEntry... layoutEntries) {
		layout = layoutEntries;
	}

	public void log() {

	}
}
