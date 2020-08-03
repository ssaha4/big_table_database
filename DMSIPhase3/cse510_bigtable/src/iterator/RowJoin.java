package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import bigt.BigT;
import bigt.Map;
import bigt.Stream;
import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
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
import index.IndexException;

public class RowJoin {

	SortMerge sm = null;
	Stream left = null;
	BigT output;
	BigT left_table;
	BigT right_table;
	int buff;
	String column;
	CondExpr[] evalExpr;
	AttrType[] attrType;
	short[] attrSize;
	Heapfile hf;
	Heapfile hf2;

	public RowJoin(int mem, String left, String right, String Column, String out)
			throws InvalidSlotNumberException, Exception {
		buff = mem;
		left_table = new BigT(left);
		right_table = new BigT(right);
		output = new BigT(out);
		column = Column;
		attrSize = new short[4];
		attrSize[0] = 22;
		attrSize[1] = 22;
		attrSize[2] = 4;
		attrSize[3] = 22;
		attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrString);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrString);
		
		
	}

	// rowJoin() function will take care of initializing stream for inner table and outer table on the basis of column filter and pass it to the sort-merge operator
	public void rowJoin() throws SortException, LowMemException, Exception {

		short[] Rsizes = new short[4];
		Rsizes[0] = 22;
		Rsizes[1] = 22;
		Rsizes[3] = 22;
		short[] Ssizes = new short[4];
		Ssizes[0] = 22;
		Ssizes[1] = 22;
		Ssizes[3] = 22;

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

		int maxbuf = (int) (buff * 0.8);
		
		//Stream for left Bigtable based on the column filter specified in the constructor 
		Stream leftstream = left_table.openStream(3, "*", column, "*", maxbuf);
		hf = new Heapfile("joinTempLeft");
		
		//Append the latest values for all the rows and append them to a heap file
		createHfRows(leftstream, hf);
		FileScan fsLeft = new FileScan("joinTempLeft", attrType, attrSize, (short) 4, 4, null, null);
		leftstream.closestream();
		
		//Initialize a stream for right big table on the basis of same column filter 
		Stream rightstream = right_table.openStream(3, "*", column, "*", maxbuf);
		hf2 = new Heapfile("joinTempRight");
		
		//Append the latest values for all the rows and append them to a heap file
		createHfRows(rightstream, hf2);
		FileScan fsRight = new FileScan("joinTempRight", attrType, attrSize, (short) 4, 4, null, null);
		rightstream.closestream();
		
		// Actual sort merge operations are being carried out in the given Class 
		sm = new SortMerge(attrType, 4, Rsizes, attrType, 4, Ssizes, 4, 22, 4, 22, maxbuf, fsLeft, fsRight, false, false,
				ascending, null, null, 4);

	}
	
	// Append maps in a particular heap file
	public void createHfRows(Stream res, Heapfile hf)
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		Map temp = null;
		temp = res.getNext();
		Map prev1 = null;

		if (temp != null)
			prev1 = new Map(temp);

		while (temp != null) {
			// All distinct rowLabels with their highest time stamps are written in hfTemp
			if (!temp.getRowLabel().equals(prev1.getRowLabel())) {
				hf.insertRecord(prev1.getMapByteArray());
			}
		
			prev1 = new Map(temp);
			temp = res.getNext();
		}

		hf.insertRecord(prev1.getMapByteArray());

	}

	// This function creates a separate Big table and append all the rows to it with a type 1 indexing
	
	public BigT construct_bigt() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, IOException, Exception {
		Map t = null;

		t = sm.get_next();
		while (t != null) {

			ArrayList<Map> sort = new ArrayList<Map>(6);
			String[] rowtoken = t.getRowLabel().split(":");

			initEval(rowtoken[0]);
			MultipleFileScan fs = new MultipleFileScan(left_table.name, attrType, attrSize, (short) 4, 4, null,evalExpr);
			Map temp = fs.get_next();
			while (temp != null) {
				
				temp.setRowLabel(t.getRowLabel());
				if (temp.getColumnLabel().equals(column)) {
					sort.add(temp);
					temp = fs.get_next();
					continue;
				}
				temp.setColumnLabel(temp.getColumnLabel()+"_Left");
				output.insertMap(temp.getMapByteArray(), 1);
				temp = fs.get_next();
			}

			fs.close();
			
			initEval(rowtoken[1]);

			MultipleFileScan fs1 = new MultipleFileScan(right_table.name, attrType, attrSize, (short) 4, 4, null,evalExpr);
			temp = fs1.get_next();
			while (temp != null) {
				temp.setRowLabel(t.getRowLabel());
				if (temp.getColumnLabel().equals(column)) {
					sort.add(temp);
					temp = fs1.get_next();
					continue;
				}
				temp.setColumnLabel(temp.getColumnLabel()+"_Right");
				output.insertMap(temp.getMapByteArray(), 1);
				temp = fs1.get_next();
			}
			
			fs1.close();
			
			int n = sort.size();
			Collections.sort(sort, getCompByName());

			Map current = null;
			int min = Math.min(n, 3);
			for (int i = 0; i < min; i++) {
				current = sort.get(i);
					output.insertMap(current.getMapByteArray(), 1);
			}
			t = sm.get_next();
		}
		sm.close();
		hf.deleteFile();
		hf2.deleteFile();
		
		return output;
	}

	public void initEval(String rowLabel) {
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

	//Print the values of a BigT 
	public void printBigT() throws JoinsException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, IOException, FileScanException, TupleUtilsException, InvalidRelation, InvalidInfoSizeException {
		MultipleFileScan fs1 = new MultipleFileScan(output.name, attrType, attrSize, (short) 4, 4, null,null);
		Map g = fs1.get_next();
		while(g!=null)
		{
			g.print();
			System.out.println();
			g = fs1.get_next();
		}
		fs1.close();
		
	}
	
	//Operator to sort a list of maps on the basis of time-stamps
	public static Comparator<Map> getCompByName()
	{   
	 Comparator comp = new Comparator<Map>(){
	     @Override
	     public int compare(Map s1, Map s2)
	     {
	    	 int t1 = 0;
			try {
				t1 = s1.getTimeStamp();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	 int t2 = 0;
			try {
				t2 = s2.getTimeStamp();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	         return t1>t2?-1:0;
	     }        
	 };
	 return comp;
	}  




}