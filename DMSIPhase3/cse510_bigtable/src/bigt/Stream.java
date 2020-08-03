package bigt;

/** JAVA */
/**
 * Scan.java-  class Scan
 *
 */

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import bigt.*;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IteratorException;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.UnpinPageException;
import heap.*;
import index.IndexException;
import index.IndexScan;
import index.InvalidSelectionException;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.MultipleFileScan;
import iterator.QueryScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

/**
 * 
 * Stream class retrieves maps in a specified order
 *
 */
public class Stream implements GlobalConst {

	private BigT bigt;
	static int j = 0;
	static int ind = 0;
	private QueryScan fscan;
//	private IndexScan iscan;
	private static short REC_LEN1 = 22;
	Sort sort = null;

	/**
	 * Constructor for stream class
	 * 
	 * @param bigT      - BigTable used in the database
	 * @param orderType - order type of results
	 * @param rowFilter - filter for the row
	 * @param colFilter - filter for the column
	 * @param valFilter - filter for the value
	 * @param numbuf    - number of buffers allocated
	 * @throws Exception 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws HFException 
	 * @throws InvalidSlotNumberException 
	 */
	public Stream(BigT bigT, int orderType, String rowFilter, String colFilter, String valFilter, int numbuf)
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {

		this.bigt = bigT;
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
		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);

		int i = 3;
		if (rowFilter.contains(",")) {
			i++;

		}
		if (colFilter.contains(",")) {
			i++;

		}
		if (valFilter.contains(",")) {
			i++;

		}
		CondExpr[] evalExpr = new CondExpr[i + 1];
		CondExpr[] indExpr1;
		CondExpr[] indExpr2;
		CondExpr[] indExpr3;
		CondExpr[] indExpr4;
		
		evalExpr[i] = null;
		
		filter(rowFilter, 1, evalExpr);
		filter(colFilter, 2, evalExpr);
		filter(valFilter, 4, evalExpr);
		
		if(rowFilter.equals("*")) {
			indExpr1=null;
		}
		else {
			if (rowFilter.contains(",")) 
		
				indExpr1 = new CondExpr[3];
		else
			indExpr1 = new CondExpr[2];
		indexFilter(rowFilter, 1, indExpr1);
		}

		ind =0;
		
		if(colFilter.equals("*")) {
			indExpr2=null;
		}
		else {
			if (colFilter.contains(",")) 
		
				indExpr2 = new CondExpr[3];
		else
			indExpr2 = new CondExpr[2];
		indexFilter(colFilter, 2, indExpr2);
		}
		ind =0;
		
		
		if (colFilter.contains("*") || rowFilter.contains("*") || colFilter.contains(",")
				|| rowFilter.contains(",")) {
			indExpr3=null;
		}
		else {
			
			indExpr3 = new CondExpr[2];
			indexFilter(colFilter + rowFilter, 5, indExpr3);

		}
	
		ind =0;
		
		
		if (valFilter.contains("*") || rowFilter.contains("*") || valFilter.contains(",")
				|| rowFilter.contains(",")) {
			indExpr4=null;
		}
		else {
			
			indExpr4 = new CondExpr[2];
			indexFilter(rowFilter+valFilter, 6, indExpr4);
		}
		
		ind =0;
		fscan = new QueryScan(bigt.name, attrType, attrSize, (short) 4, 4, null, evalExpr,indExpr1,indExpr2,indExpr3,indExpr4);
		
		
//		MultipleFileScan fscan = new MultipleFileScan(bigt.name, attrType, attrSize, (short) 4, 4, null, evalExpr);
		int maxbuf = (int) (0.8*numbuf);
		sort = new Sort(attrType, (short) 4, attrSize, fscan, orderType, order[0], REC_LEN1, maxbuf);
	
		j = 0;
		ind = 0;
	}

	/**
	 * Filter function to generate an expression on index based fields
	 * 
	 * @param filt          - filter condition
	 * @param fld           - field on which the filter is to be performed
	 * @param indExpression - filtering expression
	 */
	private void indexFilter(String filt, int fld, CondExpr[] indExpression) {
		CondExpr indExpr = new CondExpr();
	
		if (filt.startsWith("[") && filt.endsWith("]")) {
			filt = filt.substring(1, filt.length()-1);

			String operand1 = filt.split(",")[0];
			String operand2 = filt.split(",")[1];

			indExpr = new CondExpr();
			indExpr.op = new AttrOperator(AttrOperator.aopGE);
			indExpr.type1 = new AttrType(AttrType.attrSymbol);
			indExpr.type2 = new AttrType(AttrType.attrString);
			indExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			indExpr.operand2.string = operand1;
			indExpr.next = null;
			indExpression[ind] = indExpr;
			ind++;
			
			indExpr = new CondExpr();
			indExpr.op = new AttrOperator(AttrOperator.aopLE);
			indExpr.type1 = new AttrType(AttrType.attrSymbol);
			indExpr.type2 = new AttrType(AttrType.attrString);
			indExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			indExpr.operand2.string = operand2;
			indExpr.next = null;
			indExpression[ind] = indExpr;
			ind++;

		} else {
			indExpr = new CondExpr();
			indExpr.op = new AttrOperator(AttrOperator.aopEQ);
			indExpr.type1 = new AttrType(AttrType.attrSymbol);
			indExpr.type2 = new AttrType(AttrType.attrString);
			indExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			indExpr.operand2.string = filt;
			indExpr.next = null;
			indExpression[ind] = indExpr;
			ind++;
			
		}
	}

	/**
	 * Filter function to generate an expression on non index fields
	 * 
	 * @param filt           - filter condition
	 * @param fld            - field on which the filter is to be performed
	 * @param evalExpression - filtering expression
	 */
	private void filter(String filt, int fld, CondExpr[] evalExpression) {
		CondExpr evalExpr = new CondExpr();
		if (filt.equals("*") || filt.equals(null)) {
			evalExpression[j] = null;
			j++;

		} else if (filt.startsWith("[") && filt.endsWith("]")) {
			filt = filt.substring(1, filt.length() - 1);

			String operand1 = filt.split(",")[0];
			String operand2 = filt.split(",")[1];

			evalExpr = new CondExpr();
			evalExpr.op = new AttrOperator(AttrOperator.aopGE);
			evalExpr.type1 = new AttrType(AttrType.attrSymbol);
			evalExpr.type2 = new AttrType(AttrType.attrString);
			evalExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			evalExpr.operand2.string = operand1;
			evalExpr.next = null;
			evalExpression[j] = evalExpr;
			j++;
			evalExpr = new CondExpr();
			evalExpr.op = new AttrOperator(AttrOperator.aopLE);
			evalExpr.type1 = new AttrType(AttrType.attrSymbol);
			evalExpr.type2 = new AttrType(AttrType.attrString);
			evalExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			evalExpr.operand2.string = operand2;
			evalExpr.next = null;
			evalExpression[j] = evalExpr;
			j++;

		} else {
			evalExpr = new CondExpr();
			evalExpr.op = new AttrOperator(AttrOperator.aopEQ);
			evalExpr.type1 = new AttrType(AttrType.attrSymbol);
			evalExpr.type2 = new AttrType(AttrType.attrString);
			evalExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld);
			evalExpr.operand2.string = filt;
			evalExpr.next = null;
			evalExpression[j] = evalExpr;
			j++;

		}
	}

	/**
	 * Retrieve the next record in a sequential scan
	 *
	 * @param rid Record ID of the record
	 * @return the Info of the retrieved record.
	 * @throws Exception
	 * @throws JoinsException
	 * @throws LowMemException
	 * @throws UnknowAttrType
	 * @throws SortException
	 */
	public Map getNext() throws SortException, UnknowAttrType, LowMemException, JoinsException, Exception {
		return sort.get_next();
	}

	/**
	 * Closes the Scan object
	 * 
	 * @throws IOException
	 * @throws SortException
	 * @throws IndexException
	 */
	public void closestream() throws SortException, IOException, IndexException {
		sort.close();
	}
}
