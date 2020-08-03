package tests;

import java.io.BufferedReader;

import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import bigt.BigT;
//import array for bigDB
import bigt.Map;


/**
 * BatchInsert is called from MainClass when user selects option 1.
 * 
 * Here, batch insert happens from csv file given by the user to a big table
 */

public class MapInsert extends TestDriver {

	String[] args = new String[6];

	MapInsert() {

	}

	/**
	 * to insert records into Big Table
	 * 
	 * @param bigdb    - the Big Table into which you want to insert records(given
	 *                 by user)
	 * @param dataFile - the data file from where we extract records to insert into
	 *                 bigdb
	 * @param numbuf   - number of buffers allocated for batch insert
	 *
	 */
	public void insert(BigT b, String rowLabel,String colLabel,String value,int timeStamp, int numbuf, int type ) {

		Map map = null;
		try {
			if(type!=1)
			b.createTempHF(type);
			
				map = new Map();
				map.setHdr();

				// Populate the values to a map
				map.setRowLabel(rowLabel);
				map.setColumnLabel(colLabel);
				map.setTimeStamp(timeStamp);
				map.setValue(value);

				// Insert the map into bigt
				//b.insertMap(map.getMapByteArray(), type);
				
				

//			if(type!=1)
//				b.maintainSortedOrder(type, numbuf/2);
			
			if(type!=1)
			{
				b.mapInsertOrder(type,map);
			}
			else
			{
				b.insertMap(map.getMapByteArray(), type);
			}
			
			
			b.populateBtree();
			b.removeDuplicates();
			b.insertIndex(type);

//			System.out.println("Map count is " + b.getMapCnt());

		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
