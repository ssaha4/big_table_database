package iterator;

import heap.*;
import index.IndexException;
import index.IndexScan;
import global.*;
import bufmgr.*;
import diskmgr.*;

import java.lang.*;

import bigt.Map;

import java.io.*;

/**
 * open a heapfile and according to the condition expression to get output file,
 * call get_next to get all tuples
 */
public class QueryScan extends Iterator {
	private AttrType[] _in1;
	private short in1_len;
	private short[] s_sizes;
	private Heapfile f1,f2,f3,f4,f5;
	private Scan scan1,scan2,scan3,scan4,scan5;
	private IndexScan iscan;
	private IndexScan iscan2;
	private IndexScan iscan3;
	private IndexScan iscan4;
	private Map tuple1;
	private Map Jtuple;
	private int t1_size;
	private int nOutFlds;
	private CondExpr[] OutputFilter;
	public FldSpec[] perm_mat;

	/**
	 * constructor
	 * 
	 * @param file_name  heapfile to be opened
	 * @param in1[]      array showing what the attributes of the input fields are.
	 * @param s1_sizes[] shows the length of the string fields.
	 * @param len_in1    number of attributes in the input tuple
	 * @param n_out_flds number of fields in the out tuple
	 * @param proj_list  shows what input fields go where in the output tuple
	 * @param outFilter  select expressions
	 * @param indExpr 
	 * @param indExpr4 
	 * @param indExpr3 
	 * @param indExpr2 
	 * @exception IOException         some I/O fault
	 * @exception FileScanException   exception from this class
	 * @exception TupleUtilsException exception from this class
	 * @exception InvalidRelation     invalid relation
	 * @throws InvalidTypeException
	 */
	public QueryScan(String file_name, AttrType in1[], short s1_sizes[], short len_in1, int n_out_flds,
			FldSpec[] proj_list, CondExpr[] outFilter, CondExpr[] indExpr, CondExpr[] indExpr2, CondExpr[] indExpr3, CondExpr[] indExpr4)
			throws IOException, FileScanException, TupleUtilsException, InvalidRelation, InvalidTypeException {
		_in1 = in1;
		in1_len = len_in1;
		s_sizes = s1_sizes;

		Jtuple = new Map();
		Jtuple.setHdr();
		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] ts_size;
		try {

			ts_size = Maputils.setup_op_map(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);
		} catch (MapUtilsException | InvalidRelation | IOException e1) {
// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		OutputFilter = outFilter;
		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		tuple1 = new Map();

		try {
			tuple1.setHdr();
		} catch (Exception e) {
			throw new FileScanException(e, "setHdr() failed");
		}
		t1_size = tuple1.size();

		try {
			f1 = new Heapfile(file_name+"1");
			f2 = new Heapfile(file_name+"2");
			f3 = new Heapfile(file_name+"3");
			f4 = new Heapfile(file_name+"4");
			f5 = new Heapfile(file_name+"5");
		
		} catch (Exception e) {
			throw new FileScanException(e, "Create new heapfile failed");
		}

		try {
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
			scan1 = f1.openScan();

			iscan = new IndexScan(new IndexType(IndexType.Row_Index), file_name+"2", file_name + "Index2", attrType,
					attrSize, 4, 4, null, indExpr, 1, false, outFilter);
			iscan2 = new IndexScan(new IndexType(IndexType.Row_Index), file_name+"3", file_name + "Index3", attrType,
					attrSize, 4, 4, null, indExpr2, 1, false, outFilter);
			iscan3 = new IndexScan(new IndexType(IndexType.Row_Index), file_name+"4", file_name + "Index4", attrType,
					attrSize, 4, 4, null, indExpr3, 1, false, outFilter);
			iscan4 = new IndexScan(new IndexType(IndexType.Row_Index), file_name+"5", file_name + "Index5", attrType,
					attrSize, 4, 4, null, indExpr4, 1, false, outFilter);
		} catch (Exception e) {
			throw new FileScanException(e, "openScan() failed");
		}
	}

	/**
	 * @return shows what input fields go where in the output tuple
	 */
	public FldSpec[] show() {
		return perm_mat;
	}

	/**
	 * @return the result tuple
	 * @throws Exception 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws HFException 
	 * @throws InvalidSlotNumberException 
	 */
	public Map get_next() throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		MID rid = new MID();
		;

		while (true) {
			try {
				if ((tuple1 = scan1.getNext(rid)) == null) {
					 break;
				}
			} catch (InvalidInfoSizeException | IOException e) {
				e.printStackTrace();
			}

			tuple1.setHdr();
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) {
				//	 Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds); 
				Jtuple = tuple1;
				return Jtuple;
			}
		}
	
		while (true) {
			try {
				
				if ((tuple1 = iscan.get_next()) == null) {
					
					break;
				}
			} catch (InvalidInfoSizeException | IOException e) {
				e.printStackTrace();
			}

			
			tuple1.setHdr();
	
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) {
				//	 Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds); 
				Jtuple = tuple1;
				return Jtuple;
			}
		}
		
		
		while (true) {
			try {
				if ((tuple1 = iscan2.get_next()) == null) {
					break;
				}
			} catch (InvalidInfoSizeException | IOException e) {
				e.printStackTrace();
			}

			tuple1.setHdr();
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) {
				//	 Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds); 
				Jtuple = tuple1;
				return Jtuple;
			}
		}
		
		while (true) {
			try {
				if ((tuple1 = iscan3.get_next()) == null) {
					break;
				}
			} catch (InvalidInfoSizeException | IOException e) {
				e.printStackTrace();
			}

			tuple1.setHdr();
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) {
				//	 Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds); 
				Jtuple = tuple1;
				return Jtuple;
			}
		}
		
		while (true) {
			try {
				if ((tuple1 = iscan4.get_next()) == null) {
					return null;
				}
			} catch (InvalidInfoSizeException | IOException e) {
				e.printStackTrace();
			}

			tuple1.setHdr();
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) {
				//	 Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds); 
				Jtuple = tuple1;
				return Jtuple;
			}
		}
		
	}

	public MapMidPair get_nextMidPair()
			throws JoinsException, IOException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException,
			PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat {

		MID mid = new MID();
		Map map = new Map();
		MapMidPair mpair = new MapMidPair();
		
		while (true) 
		{
			try 
			{
				if ((tuple1 = scan1.getNext(mid)) == null)  break;	
			} 
			catch (InvalidInfoSizeException | IOException e) 
			{
				e.printStackTrace();
			}

			tuple1.setHdr();
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) 
			{
				// Projection.Project(tuple1, _in1, Jtuple, perm_mat, nOutFlds);
				Jtuple = tuple1;
				mpair.mid = mid;
				mpair.map = Jtuple;
				return mpair;
			}
		}
		
		while (true) 
		{
			try 
			{
				if ((tuple1 = scan2.getNext(mid)) == null) break;	
			} 
			catch (InvalidInfoSizeException | IOException e) 
			{
				e.printStackTrace();
			}

			tuple1.setHdr();
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) 
			{
				// Projection.Project(tuple1, _in1, Jtuple, perm_mat, nOutFlds);
				Jtuple = tuple1;
				mpair.mid = mid;
				mpair.map = Jtuple;
				return mpair;
			}
		}
		
		while (true) 
		{
			try 
			{
				if ((tuple1 = scan3.getNext(mid)) == null) break;	
			} 
			catch (InvalidInfoSizeException | IOException e) 
			{
				e.printStackTrace();
			}

			tuple1.setHdr();
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) 
			{
				// Projection.Project(tuple1, _in1, Jtuple, perm_mat, nOutFlds);
				Jtuple = tuple1;
				mpair.mid = mid;
				mpair.map = Jtuple;
				return mpair;
			}
		}
		
		while (true) 
		{
			try 
			{
				if ((tuple1 = scan4.getNext(mid)) == null) break;	
			} 
			catch (InvalidInfoSizeException | IOException e) 
			{
				e.printStackTrace();
			}

			tuple1.setHdr();
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) 
			{
				// Projection.Project(tuple1, _in1, Jtuple, perm_mat, nOutFlds);
				Jtuple = tuple1;
				mpair.mid = mid;
				mpair.map = Jtuple;
				return mpair;
			}
		}
		
		while (true) 
		{
			try 
			{
				if ((tuple1 = scan5.getNext(mid)) == null) { return null;}	
			} 
			catch (InvalidInfoSizeException | IOException e) 
			{
				e.printStackTrace();
			}

			tuple1.setHdr();
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) 
			{
				// Projection.Project(tuple1, _in1, Jtuple, perm_mat, nOutFlds);
				Jtuple = tuple1;
				mpair.mid = mid;
				mpair.map = Jtuple;
				return mpair;
			}
		}
		
	
	}

	/**
	 * implement the abstract method close() from super class Iterator to finish
	 * cleaning up
	 * @throws IOException 
	 * @throws IndexException 
	 */
	public void close() throws IndexException, IOException {

		if (!closeFlag) {
			scan1.closescan();
			iscan.close();
			iscan2.close();
			iscan3.close();
			iscan4.close();
			closeFlag = true;
		}
	}

}