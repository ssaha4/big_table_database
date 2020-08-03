package tests;

import java.io.IOException;

import bigt.BigT;
import bigt.Map;
import bigt.Stream;
import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.MID;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidInfoSizeException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.MultipleFileScan;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.WrongPermat;

public class RowSort {

	CondExpr[] evalExpr;
	AttrType[] attrType;
	short[] attrSize;
	TupleOrder[] order;
	BigT obt;
	
	/**
	 * Initialise the required attributes for Row sort
	 */
	RowSort(){
		attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrString);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrString);
		attrSize = new short[4];
		attrSize[0] = 22;
		attrSize[1] = 22;
		attrSize[2] = 4;
		attrSize[3] = 22;
		order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);
	}
	
	/**
	 * Function to perform row Sort
	 * @param ib - input BigT
	 * @param ob - Output BigT
	 * @param columnName - COlumn on which Rowsort needs to be performed
	 * @param orderType - Ascending or Descenidng
	 * @param numBuf - number of buffers
	 * @throws InvalidSlotNumberException
	 * @throws HFException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws Exception
	 */
	public void performRowSort(BigT ib, BigT ob, String columnName,String orderType, int numBuf)
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception 
	{
		
		this.obt = ob;
		int maxBuf = (int) (0.80*numBuf);
		Heapfile hfRow = new Heapfile("allRows1");
		Stream res1 = ib.openStream(3, "*", "*", "*", maxBuf);
		createHfRows(res1,hfRow);
		res1.closestream();
		
		
		//Based on the given column we get all the maps sorted based on rowLabel + timeStamp 
		Stream res = ib.openStream(3, "*", columnName, "*", maxBuf);
		Heapfile hfTemp = new Heapfile("dummy");
		createHfRows(res,hfTemp);
		res.closestream();
		
		//Now an iterator for this row sort has to be created so we initialze a file scan with output bigt and return get_next()
				copyUnFilteredRows(hfRow,hfTemp,ib,ob);
		
		
		//Now HFTemp has all the records(based on given column filter) with distinct rows and now we sort them based on values
		FileScan fscan = new FileScan("dummy", attrType, attrSize, (short) 4, 4, null, null);
		Sort sort = null;
		if(orderType.equals("asc")) {
		 sort = new Sort(attrType, (short) 4, attrSize, fscan, 6, order[0], 22, maxBuf);
		}
		else {
			sort = new Sort(attrType, (short) 4, attrSize, fscan, 6, order[1], 22, maxBuf);
		}
		Map sortValue = sort.get_next();
		String rowLabel;
		
		//Now we have a found row based on the sortOrder(ValueBased) retrieve all the matching maps with these row labels by passing an evalExpression with this rowLabel and pass it to file scan  
		while(sortValue!=null)
		{
			rowLabel = sortValue.getRowLabel();
			initEval(rowLabel);
			
			//get all rows with the given rowLabel
			MultipleFileScan fs = new MultipleFileScan(ib.name, attrType, attrSize, (short) 4, 4, null, evalExpr);
			Map temp_map = fs.get_next();
			
			//Now all the maps with the given rowLabel is inserted into output bigt
			while(temp_map!=null)
			{
				byte[] mapArray = temp_map.getMapByteArray();
				ob.insertMap(mapArray,1);
				temp_map=fs.get_next();
			}
			fs.close();
		
			sortValue = sort.get_next();
			
		}
		sort.close();

		MultipleFileScan fsTest = new MultipleFileScan(ob.name, attrType, attrSize, (short) 4, 4, null, null);
		Map temp_map = fsTest.get_next();
		
		while(temp_map!=null)
		{
			temp_map.print();
			temp_map = fsTest.get_next();
			System.out.println("");
		}
		fsTest.close();

		hfRow.deleteFile();
		hfTemp.deleteFile();
	}
	
	/**
	 * evaluation expression for stream is constructed.
	 * @param rowLabel
	 */
	public void initEval(String rowLabel)
	{
		evalExpr = new CondExpr[4];
		evalExpr[3] = null;
		
		evalExpr[0] = new CondExpr();
		evalExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
		evalExpr[0].type1 = new AttrType(AttrType.attrSymbol);
		evalExpr[0].type2 = new AttrType(AttrType.attrString);
		evalExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		evalExpr[0].next = null;
		evalExpr[0].operand2.string = rowLabel;
		evalExpr[1] = null;
		evalExpr[2] = null;
	}
	
	/**
	 * All distinct RowLabels with Higher timestamp is inserted into a temporary heap file for the purpose of RowSort
	 * @param res - Stream of maps
	 * @param hf - temprary Heap file
	 * @throws InvalidSlotNumberException
	 * @throws HFException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws Exception
	 */
	public void createHfRows(Stream res, Heapfile hf) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		Map temp = null;
		temp = res.getNext();
		Map prev1 = null;

		if (temp != null)
			prev1 = new Map(temp);
		
		while (temp != null) 
		{
			//All distinct rowLabels with their highest time stamps are written in hfTemp  
			if (!temp.getRowLabel().equals(prev1.getRowLabel())) {
				hf.insertRecord(prev1.getMapByteArray());
			}

			prev1 = new Map(temp);
			temp = res.getNext();
		}
		hf.insertRecord(prev1.getMapByteArray());
	
	}
	
	
	/**
	 * The remaining rows other than the rows obtained by the rowsort with specific column label are written into the output bigT
	 * @param hfRow - Temprary heapfile with all Rowlabels 
	 * @param hfTemp - Temp heap file
	 * @param ib - input bigt 
	 * @param ob - output bigt
	 * @throws InvalidSlotNumberException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws Exception
	 */
	public void copyUnFilteredRows(Heapfile hfRow, Heapfile hfTemp,BigT ib, BigT ob) throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception
	{
		Scan sc1 = hfRow.openScan();
		Scan sc2 = hfTemp.openScan();
		MID mid1 = new MID();
		MID mid2 = new MID();
		String r1,r2;
		Map m1 = sc1.getNext(mid1);
		Map m2 = sc2.getNext(mid2);
		
		while(m1!=null)
		{
			r1 = m1.getRowLabel();
			
			if(m2!=null)
				r2 = m2.getRowLabel();
			else
				r2 = "";
			
			if(!r1.equals(r2))
			{
//				System.out.println("rowLabel1 is "+r1+" rowLabel 2 is "+r2);
				initEval(r1);
				MultipleFileScan fsDummy = new MultipleFileScan(ib.name, attrType, attrSize, (short) 4, 4, null, evalExpr);
				Map fsDummyMap = fsDummy.get_next();

				while(fsDummyMap!=null)
				{
					ob.insertMap(fsDummyMap.getMapByteArray(),1);
					fsDummyMap = fsDummy.get_next();
				}
				fsDummy.close();
			}
	
			else
				m2 = sc2.getNext(mid2);
			
			m1 = sc1.getNext(mid1);
		}
		
		sc1.closescan();
		sc2.closescan();
		
		
	}
	
	/**
	 * Iterator to access all the maps from output bigT
	 * @throws FileScanException
	 * @throws TupleUtilsException
	 * @throws InvalidRelation
	 * @throws InvalidTypeException
	 * @throws IOException
	 * @throws JoinsException
	 * @throws InvalidTupleSizeException
	 * @throws PageNotReadException
	 * @throws PredEvalException
	 * @throws UnknowAttrType
	 * @throws FieldNumberOutOfBoundException
	 * @throws WrongPermat
	 * @throws InvalidInfoSizeException
	 */
	public void getNext() throws FileScanException, TupleUtilsException, InvalidRelation, InvalidTypeException, IOException, JoinsException, InvalidTupleSizeException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, InvalidInfoSizeException {
		
		MultipleFileScan fsTest = new MultipleFileScan(obt.name, attrType, attrSize, (short) 4, 4, null, null);
		Map temp_map = fsTest.get_next();
		
		while(temp_map!=null)
		{
			temp_map.print();
			temp_map = fsTest.get_next();
			System.out.println("");
		}
		fsTest.close();
		
	}
	
	
	
	
	
}
