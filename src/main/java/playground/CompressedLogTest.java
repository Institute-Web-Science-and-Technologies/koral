package playground;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.CompressedLogReader;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.CompressedLogWriter;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.ElementType;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.LogRow;

public class CompressedLogTest {

	public static void main(String[] args) {
		File storageFile = new File("/tmp/compressedLog");
		if (storageFile.exists()) {
			storageFile.delete();
		}
		Map<Integer, Map<String, ElementType>> rowLayouts = new HashMap<>();

		Map<String, ElementType> layout1 = new TreeMap<>();
		layout1.put("int", ElementType.INTEGER);
		layout1.put("bit1", ElementType.BIT);
		layout1.put("byte", ElementType.BYTE);
		layout1.put("bit2", ElementType.BIT);
		rowLayouts.put(0, layout1);

		Map<String, ElementType> layout2 = new TreeMap<>();
		layout2.put("2byte1", ElementType.BYTE);
		layout2.put("2byte2", ElementType.BYTE);
		layout2.put("2bit", ElementType.BIT);
		layout2.put("2int", ElementType.INTEGER);
		layout2.put("2bit2", ElementType.BIT);
		rowLayouts.put(1, layout2);

		CompressedLogWriter writer = new CompressedLogWriter(storageFile, rowLayouts);

		Map<String, Object> data = new HashMap<>();
		data.put("int", 6512);
		data.put("bit1", (byte) 1);
		data.put("byte", (byte) 38);
		data.put("bit2", (byte) 0);
		writer.log(0, data);
		data.clear();
		writer.close();

		writer.open();
		data.put("2byte1", (byte) 46);
		data.put("2byte2", (byte) 112);
		data.put("2bit", (byte) 1);
		data.put("2int", 1056);
		data.put("2bit2", (byte) 1);
		writer.log(1, data);
		writer.close();

		CompressedLogReader reader = new CompressedLogReader(storageFile);
		LogRow logRow;
		while ((logRow = reader.readRow()) != null) {
			System.out.println(logRow.getRowType());
			for (Entry<String, Object> entry : logRow.getData().entrySet()) {
				System.out.println(entry.getKey() + " : " + entry.getValue());
			}
		}
		reader.close();
	}

}
