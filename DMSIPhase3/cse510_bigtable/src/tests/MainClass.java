package tests;

import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import bigt.BigT;
import bigt.Map;
import bigt.Stream;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import diskmgr.Pcounter;
import global.AttrType;
import global.SystemDefs;
import global.TupleOrder;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidInfoSizeException;
import heap.InvalidSlotNumberException;
import heap.InvalidTypeException;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.InvalidRelation;
import iterator.LowMemException;
import iterator.RowJoin;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;

/**
 * This MainClass allows user to select from two options to perform batch insert
 * and query retrieval
 */

public class MainClass {

	static Set<String> bigtNames = new HashSet<String>();

	public static void main(String[] args) {

		String dbpath;

		@SuppressWarnings("unused")
		SystemDefs sysDef = null;

		dbpath = "/tmp/" + "database" + System.getProperty("user.name") + ".minibase-db";
		sysDef = new SystemDefs(dbpath, 100000, 1000, "Clock");

		Scanner in = new Scanner(System.in);
		System.out.println("Please enter option:\n\n" + "[1] Batch Insert\n" + "[2] Map Insert\n" + "[3] Query\n"
				+ "[4] Get Counts\n" + "[5] Row Join\n" + "[6] Row Sort\n" + "[7] Exit");
		String msg = in.nextLine();
		int input = Integer.valueOf(msg);
		int numbuf = 0;

		try {

			while (input != 7) 
			{
				switch (input) {

				case 1: {

					System.out.println(
							"Enter Parameters for Batch Insert \n BatchInsert \t filePath \t Type \t BigtName \t Buffers");
					Pcounter.initialize();
					msg = in.nextLine();
					String[] str = msg.split(" ");
					args = str;
					String dataFile = "/Users/sruthi/Downloads/Dataset/" + args[1]; /// Users/sruthi/Downloads/Dataset/
					// dont remove this --> /Users/saiuttej/Documents/DBMSI/project material/
					// ///Users/sindu/Documents/Dataset_revised/" + args[1];

					int type = Integer.parseInt(args[2]);
					String bigTableName = args[3];
					bigtNames.add(bigTableName);
					numbuf = Integer.parseInt(args[4]);

					SystemDefs.JavabaseBM.setNumBuffers(numbuf);

					BatchInsert btch = new BatchInsert();
					BigT b = new BigT(bigTableName);

					long startTime = System.nanoTime();
					btch.insert(b, dataFile, numbuf, type);
					System.out.println("Map Count for given bigt is " + b.getMapCnt());
					long endTime = System.nanoTime();
					System.out.println(
							"Time Taken for batch insert operation is " + ((endTime - startTime) / 1000000000) + " s");
					System.out.println("no, of pages written are   " + Pcounter.wcounter);
					System.out.println("no, of pages read are  " + Pcounter.rcounter);
					break;
				}

				case 2: {

					Pcounter.initialize();
					System.out.println(
							"Enter Parameters for Map Insert \n MapInsert \t Row \t Column \t Value \t Timestamp \t type \t Bigt \t Buffers");
					msg = in.nextLine();
					String[] str = msg.split(" ");
					args = str;
					String rowval = args[1];
					String colval = args[2];
					String value = args[3];
					int timestmp = Integer.parseInt(args[4]);
					int type = Integer.parseInt(args[5]);
					String bigTableName = args[6];
					bigtNames.add(bigTableName);
					numbuf = Integer.parseInt(args[7]);

					BigT b = new BigT(bigTableName);

					Map m = new Map();
					m.setHdr();
					m.setRowLabel(rowval);
					m.setColumnLabel(colval);
					m.setTimeStamp(timestmp);
					m.setValue(value);

					SystemDefs.JavabaseBM.setNumBuffers(numbuf);

					long startTime = System.nanoTime();

					if (type != 1)
						b.mapInsertOrder(type, m);
					else
						b.insertMap(m.getMapByteArray(), type);

					b.removeDuplicatesMapInsert(rowval, colval);

					 //b.scanBigT();
					 b.getMapCnt();

					long endTime = System.nanoTime();
					System.out.println(
							"Time Taken for map insert operation is " + ((endTime - startTime) / 1000000000) + " s");
					System.out.println("no, of pages written are   " + Pcounter.wcounter);
					System.out.println("no, of pages read are  " + Pcounter.rcounter);
					break;

				}

				case 3: {
					Pcounter.initialize();
					System.out.println(
							"Enter Parameters for Query \n Query \t BigTName \t OrderType \t Row Filter \t Column Filter \t Value Filter \tBuffers");
					msg = in.nextLine();
					String[] str1 = msg.split(" ");
					args = str1;
					int queryBuf = Integer.parseInt(args[6]);

					SystemDefs.JavabaseBM.setNumBuffers(queryBuf);

					Query query = new Query();
					long startTime = System.nanoTime();

					BigT bt = new BigT(args[1]);
					query.retrieve(bt, Integer.parseInt(args[2]), args[3], args[4], args[5], queryBuf);

					long endTime = System.nanoTime();
					System.out.println("Time Taken for Query Retrieval operation is "
							+ ((endTime - startTime) / 1000000000) + " s");
					System.out.println("no, of pages written are   " + Pcounter.wcounter);
					System.out.println("no, of pages read are  " + Pcounter.rcounter);

					break;
				}

				case 4: {
					System.out.println("Enter Parameters for Getting Counts \n getCounts  \tBuffers");
					Pcounter.initialize();
					msg = in.nextLine();
					String[] str1 = msg.split(" ");
					args = str1;
					numbuf = Integer.parseInt(args[1]);
					long startTime = System.nanoTime();

					SystemDefs.JavabaseBM.setNumBuffers(numbuf);
					getCounts(numbuf);
					long endTime = System.nanoTime();
					System.out.println(
							"Time Taken for Get Counts operation is " + ((endTime - startTime) / 1000000000) + " s");
					System.out.println("no, of pages written are   " + Pcounter.wcounter);
					System.out.println("no, of pages read are  " + Pcounter.rcounter);

					break;
				}

				case 5: {
					Pcounter.initialize();
					System.out.println(
							"Enter Parameters for Row Join \n RowJoin \t BigTable 1 \t BigTable 2 \t Output BigT \t Column \t Buffers");
					msg = in.nextLine();
					String[] tokens = msg.split(" ");
					args = tokens;
					String Bigtable1 = args[1];
					String Bigtable2 = args[2];
					String OutBtname = args[3];
					bigtNames.add(OutBtname);
					String Columnname = args[4];
					int buf = Integer.parseInt(args[5]);
					SystemDefs.JavabaseBM.setNumBuffers(buf);

					long startTime = System.nanoTime();
					RowJoin rj = new RowJoin(buf, Bigtable1, Bigtable2, Columnname, OutBtname);

					rj.rowJoin();

					BigT result = rj.construct_bigt();
					rj.printBigT();
					long endTime = System.nanoTime();
					System.out.println("Time Taken for Query Retrieval operation is "
							+ ((endTime - startTime) / 1000000000) + " s");
					System.out.println("no, of pages written are   " + Pcounter.wcounter);
					System.out.println("no, of pages read are  " + Pcounter.rcounter);

					break;
				}

				case 6: {
					System.out.println(
							"Enter Parameters for Row Sort \n RowSort \t InputBigTable \t Output BigT \t Column \t Order \t Buffers ");
					Pcounter.initialize();

					msg = in.nextLine();
					String[] str1 = msg.split(" ");
					args = str1;
					RowSort rowSort = new RowSort();
					BigT ibt = new BigT(args[1]);
					BigT obt = new BigT(args[2]);
					bigtNames.add(args[2]);
					int numBuf = Integer.parseInt(args[5]);
					SystemDefs.JavabaseBM.setNumBuffers(numBuf);
					long startTime = System.nanoTime();
					rowSort.performRowSort(ibt, obt, args[3], args[4], numBuf);
					long endTime = System.nanoTime();
					System.out.println("Time Taken for Query Retrieval operation is "
							+ ((endTime - startTime) / 1000000000) + " s");
					System.out.println("no, of pages written are   " + Pcounter.wcounter);
					System.out.println("no, of pages read are  " + Pcounter.rcounter);

					break;
				}

				case 7:
					break;

				}

				System.out.println("Please enter option:\n\n" + "[1] Batch Insert\n" + "[2] Map Insert\n"
						+ "[3] Query\n" + "[4] Get Counts\n" + "[5] Row Join\n" + "[6] Row Sort\n" + "[7] Exit");
				msg = in.nextLine();
				input = Integer.valueOf(msg);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		in.close();
	}

	/**
	 * Get the appropriate Counts for each bigt
	 * @param numbuf - number of buffers for the operation
	 * @throws SortException
	 * @throws LowMemException
	 * @throws Exception
	 */

	public static void getCounts(int numbuf) throws SortException, LowMemException, Exception {

		AttrType[] attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrString);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrString);
		short[] attrSize = new short[4];
		attrSize[0] = 22;
		attrSize[1] = 22;
		attrSize[2] = 4;
		attrSize[3] = 22;

		for (String bigt : bigtNames) {
			
			BigT b = new BigT(bigt);
			int mc = b.getMapCnt();
			SystemDefs.JavabaseBM.setNumBuffers(numbuf);
			int rc = b.getRowCnt(numbuf);
			SystemDefs.JavabaseBM.setNumBuffers(numbuf);
			int cc = b.getColumnCnt(numbuf);
			
			System.out.println("----------------------------------------");
			System.out.println("Bigt Name : "+ bigt + "\n Map Count : "+mc+ "\n Row Count : "+rc+ "\n Column Count : "+cc);
			System.out.println();
		}

		
	}

}
