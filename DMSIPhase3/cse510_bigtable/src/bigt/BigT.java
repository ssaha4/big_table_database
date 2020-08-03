package bigt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import btree.AddFileEntryException;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFileEntryException;
import btree.DeleteRecException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.ScanDeleteException;
import btree.ScanIteratorException;
import btree.StringKey;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.MID;
import global.MapMidPair;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidInfoSizeException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.MultipleFileScan;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.WrongPermat;

/**
 * 
 * Creation of a bigTable
 *
 */
public class BigT {

	public String name;
	public int type;
	public Heapfile temp;
	public Heapfile _hf1;
	public Heapfile _hf2;
	public Heapfile _hf3;
	public Heapfile _hf4;
	public Heapfile _hf5;
	public BTreeFile _bf2 = null;
	public BTreeFile _bf3 = null;
	public BTreeFile _bf4 = null;
	public BTreeFile _bf5 = null;
	public BTreeFile _bftemp = null;
	MultipleFileScan fscan;
	FileScan hfscan;
	IndexScan iscan;
	FileScan fs;
	Scan scan;
	CondExpr[] expr;
	AttrType[] attrType;
	short[] attrSize;
	String key;

	/**
	 * 
	 * @param name - name of the bigT
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws GetFileEntryException
	 * @throws ConstructPageException
	 * @throws AddFileEntryException
	 * @throws FileScanException
	 * @throws TupleUtilsException
	 * @throws InvalidRelation
	 * @throws InvalidTypeException
	 */
	public BigT(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
			GetFileEntryException, ConstructPageException, AddFileEntryException, FileScanException,
			TupleUtilsException, InvalidRelation, InvalidTypeException {

		this.name = name;

		this.temp = new Heapfile("temp_new");
		this._hf1 = new Heapfile(name + "1"); // type1
		this._hf2 = new Heapfile(name + "2"); // type2
		this._hf3 = new Heapfile(name + "3"); // type3
		this._hf4 = new Heapfile(name + "4"); // type4
		this._hf5 = new Heapfile(name + "5"); // type5

		this._bf2 = new BTreeFile(name + "Index2", AttrType.attrString, 22, 0);
		this._bf3 = new BTreeFile(name + "Index3", AttrType.attrString, 22, 0);
		this._bf4 = new BTreeFile(name + "Index4", AttrType.attrString, 44, 0);
		this._bf5 = new BTreeFile(name + "Index5", AttrType.attrString, 44, 0);

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

	}

	/**
	 * 
	 * Delete the bigt within the Database
	 * 
	 * @throws InvalidSlotNumberException
	 * @throws FileAlreadyDeletedException
	 * @throws InvalidInfoSizeException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws IteratorException
	 * @throws UnpinPageException
	 * @throws FreePageException
	 * @throws DeleteFileEntryException
	 * @throws ConstructPageException
	 * @throws PinPageException
	 */
	void deleteBigT() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidInfoSizeException,
			HFBufMgrException, HFDiskMgrException, IOException, IteratorException, UnpinPageException,
			FreePageException, DeleteFileEntryException, ConstructPageException, PinPageException {

		_hf1.deleteFile();
		_hf2.deleteFile();
		_hf3.deleteFile();
		_hf4.deleteFile();
		_hf5.deleteFile();

		_bf2.destroyFile();
		_bf3.destroyFile();
		_bf4.destroyFile();
		_bf5.destroyFile();

	}

	/**
	 * Get the count of total number of maps
	 * 
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidInfoSizeException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws IOException
	 */
	public int getMapCnt() throws InvalidSlotNumberException, InvalidInfoSizeException, HFDiskMgrException,
			HFBufMgrException, IOException {
		int count1 = _hf1.getRecCnt();
		int count2 = _hf2.getRecCnt();
		int count3 = _hf3.getRecCnt();
		int count4 = _hf4.getRecCnt();
		int count5 = _hf5.getRecCnt();
		System.out.println("in hf1 " + count1);
		System.out.println("in hf2 " + count2);
		System.out.println("in hf3 " + count3);
		System.out.println("in hf4 " + count4);
		System.out.println("in hf5 " + count5);
		return count1+count2+count3+count4+count5;
	}

	/**
	 * Gets the total number of unique rows in the database
	 * 
	 * @param numbuf - number of buffers used for sorting functionality to get the
	 *               row count
	 * @return
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws JoinsException
	 * @throws Exception
	 */
	public int getRowCnt(int numbuf) throws UnknowAttrType, LowMemException, JoinsException, Exception {
		/*
		 * Stream of maps from bigt is opened and is then sorted to count the unique number of rows in it/
		 */
		Map map = new Map();
		Stream res = new Stream(this, 3, "*", "*", "*", numbuf);
		int count = 0;
		map = res.getNext();
		String prev = "";
		String curr = "";
		if (map != null) {
			prev = map.getRowLabel();
		}
		while (map != null) {
			curr = map.getRowLabel();
			if (!curr.equals(prev)) 
				count++;
			
			prev = curr;

			map = res.getNext();

		}
		if (curr.equals(prev)) {
			count++;

		}
		res.closestream();
		return count;
	}

	/**
	 * 
	 * Gets the total number of unique columns in the database
	 * 
	 * @param numbuf - number of buffers used for sorting functionality to get the
	 *               column count
	 * @return
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws JoinsException
	 * @throws Exception
	 */
	public int getColumnCnt(int numbuf) throws UnknowAttrType, LowMemException, JoinsException, Exception {
		/*
		 * Stream of maps from bigt is opened and is then sorted to count the unique number of columns in it/
		 */
		Map map = new Map();
		Stream res = new Stream(this, 4, "*", "*", "*", numbuf);
		int count = 0;
		map = res.getNext();
		String prev = "";
		String curr = "";
		if (map != null) {
			prev = map.getColumnLabel();
			
		}
		while (map != null) {
			curr = map.getColumnLabel();
			if (!curr.equals(prev))
				count++;

			prev = curr;

			map = res.getNext();

		}
		if (curr.equals(prev)) {
			count++;

		}
		res.closestream();
		return count;
	}

	/**
	 * Generate the index files for the maps corresponding to the desired type in all the heap files in bigT.
	 * 
	 * 
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws IOException
	 * @throws FieldNumberOutOfBoundException
	 * @throws InvalidInfoSizeException
	 * @throws FreePageException
	 * @throws DeleteFileEntryException
	 * @throws GetFileEntryException
	 * @throws AddFileEntryException
	 * @throws ReplacerException 
	 * @throws HashEntryNotFoundException 
	 * @throws InvalidFrameNumberException 
	 * @throws PageUnpinnedException 
	 */
	public void insertIndex() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException, FieldNumberOutOfBoundException, InvalidInfoSizeException,
			FreePageException, DeleteFileEntryException, GetFileEntryException, AddFileEntryException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		
		MID mid = new MID();
		String key = null;
		Map temp = null;
		
		this._bf2 = new BTreeFile(name + "Index2", AttrType.attrString, 22, 0);
		this._bf3 = new BTreeFile(name + "Index3", AttrType.attrString, 22, 0);
		this._bf4 = new BTreeFile(name + "Index4", AttrType.attrString, 44, 0);
		this._bf5 = new BTreeFile(name + "Index5", AttrType.attrString, 44, 0);

		{
			Scan scan2 = new Scan(_hf2);
			temp = scan2.getNext(mid);
			while (temp != null) {
				key = temp.getRowLabel();
				_bf2.insert(new StringKey(key), mid);
				temp = scan2.getNext(mid);
			}
			scan2.closescan();
			_bf2.close();
		} 

		{
			Scan scan3 = new Scan(_hf3);
			temp = scan3.getNext(mid);
			while (temp != null) {
				key = temp.getColumnLabel();
				_bf3.insert(new StringKey(key), mid);
				temp = scan3.getNext(mid);
			}
			scan3.closescan();
			_bf3.close();
		} 

	{
			Scan scan4 = new Scan(_hf4);
			temp = scan4.getNext(mid);
			while (temp != null) {
				key = temp.getColumnLabel() + temp.getRowLabel();
				_bf4.insert(new StringKey(key), mid);
				temp = scan4.getNext(mid);
			}
			scan4.closescan();
			_bf4.close();
		} 

		{
			Scan scan5 = new Scan(_hf5);
			temp = scan5.getNext(mid);
			while (temp != null) {
				key = temp.getRowLabel() + temp.getValue();
				_bf5.insert(new StringKey(key), mid);
				temp = scan5.getNext(mid);
			}
			scan5.closescan();
			_bf5.close();
		}
	}
	
	
	
	
	
	
	/**
	 * Insert indexes for the specific file where in the map is insreted
	 * @param type - type of file
	 * @param m - Map to be inserted
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws IOException
	 * @throws FieldNumberOutOfBoundException
	 * @throws InvalidInfoSizeException
	 * @throws FreePageException
	 * @throws DeleteFileEntryException
	 * @throws GetFileEntryException
	 * @throws AddFileEntryException
	 * @throws PageUnpinnedException
	 * @throws InvalidFrameNumberException
	 * @throws HashEntryNotFoundException
	 * @throws ReplacerException
	 */
	public void insertIndexMapInsert(int type, Map m) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
	IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
	NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
	LeafDeleteException, InsertException, IOException, FieldNumberOutOfBoundException, InvalidInfoSizeException,
	FreePageException, DeleteFileEntryException, GetFileEntryException, AddFileEntryException,
	PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {

		MID mid = new MID();
		String key = null;
		Map temp = null;

		if (type == 2) {
			key = m.getRowLabel();
			_bf2.insert(new StringKey(key), mid);
		}

		else if (type == 3) {
			key = m.getColumnLabel();
			_bf3.insert(new StringKey(key), mid);
		}

		else if (type == 4) {
			key = m.getRowLabel() + m.getColumnLabel();
			_bf4.insert(new StringKey(key), mid);
		}

		else {
			key = m.getRowLabel() + m.getValue();
			_bf5.insert(new StringKey(key), mid);
		}
	
}
	/**
	 * Initialize a map in the database
	 * 
	 * @param mapPtr
	 * @return
	 * @throws InvalidTypeException
	 * @throws IOException
	 */
	public Map constructMap(byte[] mapPtr) throws InvalidTypeException, IOException {
		Map map = new Map(mapPtr, 0);
		map.setHdr();
		return map;
	}

	
	//inserting the map into the temporary heap file for types 2,3,4,5 and into actual heap file _hf1 for type1
	/**
	 * Insert data given into bytes in the map
	 * 
	 * @param mapPtr - data to be inserted in bytes
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws Exception
	 */
	public MID insertMap(byte[] mapPtr, int type) throws InvalidSlotNumberException, InvalidTupleSizeException,
			HFException, HFBufMgrException, HFDiskMgrException, Exception {
		MID mid;
		if (type == 1) {
			mid = _hf1.insertRecord(mapPtr);
		} else {
			mid = temp.insertRecord(mapPtr);
		}
		return mid;
	}
	
	
	/**
	 * This function initializes a temporary heap file that is used for versioning.
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws IOException
	 * @throws FileScanException
	 * @throws TupleUtilsException
	 * @throws InvalidRelation
	 * @throws InvalidTypeException
	 * @throws JoinsException
	 * @throws InvalidTupleSizeException
	 * @throws PageNotReadException
	 * @throws PredEvalException
	 * @throws UnknowAttrType
	 * @throws FieldNumberOutOfBoundException
	 * @throws WrongPermat
	 * @throws GetFileEntryException
	 * @throws AddFileEntryException
	 * @throws InvalidSlotNumberException
	 * @throws InvalidInfoSizeException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 */
	public void populateBtree() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
	IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
	NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
	LeafDeleteException, InsertException, IOException, FileScanException, TupleUtilsException, InvalidRelation,
	InvalidTypeException, JoinsException, InvalidTupleSizeException, PageNotReadException, PredEvalException,
	UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, GetFileEntryException, AddFileEntryException, InvalidSlotNumberException, InvalidInfoSizeException, HFDiskMgrException, HFBufMgrException {

	_bftemp = new BTreeFile(name + "Temp", AttrType.attrString, 64, 1);

	fs = new FileScan(name+"1", attrType, attrSize, (short) 4, 4, null, null);
	MapMidPair mpair = fs.get_nextMidPair();
	while (mpair != null) {
		String s = String.format("%06d", mpair.map.getTimeStamp());
		String key = mpair.map.getRowLabel() + mpair.map.getColumnLabel() + "%" + s+"%1";
		_bftemp.insert(new StringKey(key), mpair.mid);
		mpair = fs.get_nextMidPair();
	}
	
	fs.close();
	
	
	fs = new FileScan(name+"2", attrType, attrSize, (short) 4, 4, null, null);
	mpair = fs.get_nextMidPair();
	while (mpair != null) {
		String s = String.format("%06d", mpair.map.getTimeStamp());
		String key = mpair.map.getRowLabel() + mpair.map.getColumnLabel() + "%" + s+"%2";
		_bftemp.insert(new StringKey(key), mpair.mid);
		mpair = fs.get_nextMidPair();
	}
	
	fs.close();
	fs = new FileScan(name+"3", attrType, attrSize, (short) 4, 4, null, null);
	mpair = fs.get_nextMidPair();
	while (mpair != null) {
		String s = String.format("%06d", mpair.map.getTimeStamp());
		String key = mpair.map.getRowLabel() + mpair.map.getColumnLabel() + "%" + s+"%3";
		_bftemp.insert(new StringKey(key), mpair.mid);
		mpair = fs.get_nextMidPair();
	}
	
	fs.close();
	fs = new FileScan(name+"4", attrType, attrSize, (short) 4, 4, null, null);
	mpair = fs.get_nextMidPair();
	while (mpair != null) {
		String s = String.format("%06d", mpair.map.getTimeStamp());
		String key = mpair.map.getRowLabel() + mpair.map.getColumnLabel() + "%" + s+"%4";
		_bftemp.insert(new StringKey(key), mpair.mid);
		mpair = fs.get_nextMidPair();
	}
	
	fs.close();
	fs = new FileScan(name+"5", attrType, attrSize, (short) 4, 4, null, null);
	mpair = fs.get_nextMidPair();
	while (mpair != null) {
		String s = String.format("%06d", mpair.map.getTimeStamp());
		String key = mpair.map.getRowLabel() + mpair.map.getColumnLabel() + "%" + s+"%5";
		_bftemp.insert(new StringKey(key), mpair.mid);
		mpair = fs.get_nextMidPair();
	}
	
	fs.close();

}
	
	/**
	 * Used to return the least timestamp for the purpose of versioning
	 * @param mpair - List of map and mid pairs
	 * @return
	 * @throws IOException
	 */
	public int findleastTime(List<MapMidPair> mpair) throws IOException
	{
		int ind = -1;
		List<Integer> l = new ArrayList<>();
		l.add(mpair.get(0).map.getTimeStamp());
		l.add(mpair.get(1).map.getTimeStamp());
		l.add(mpair.get(2).map.getTimeStamp());
		l.add(mpair.get(3).map.getTimeStamp());
		int i = Collections.min(l);
		return l.indexOf(i);
	}
	
	
	/**
	 * Function to remove duplicates on Map Insert
	 * @param rowLabel - RowLabel of the Map inserted
	 * @param colLabel - ColLabel of the Map inserted
	 * @throws InvalidSlotNumberException
	 * @throws HFException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws Exception
	 */
	public void removeDuplicatesMapInsert(String rowLabel, String colLabel) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		/**
		 * Get all the maps within the database and find the map to be deleted and teh corresponidng file in which it needs to be deleted and appropriately update the index file
		 */
		CondExpr evalExpr[] = new CondExpr[3]; 
		
		evalExpr[0] = new CondExpr();
		evalExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
		evalExpr[0].type1 = new AttrType(AttrType.attrSymbol);
		evalExpr[0].type2 = new AttrType(AttrType.attrString);
		evalExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		evalExpr[0].operand2.string = rowLabel;
		
		evalExpr[1] = new CondExpr();
		evalExpr[1].op = new AttrOperator(AttrOperator.aopEQ);
		evalExpr[1].type1 = new AttrType(AttrType.attrSymbol);
		evalExpr[1].type2 = new AttrType(AttrType.attrString);
		evalExpr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		evalExpr[1].operand2.string = colLabel;
		
		evalExpr[2] = null;
		int count = 0;
			
		MultipleFileScan fscan1 = new MultipleFileScan(name, attrType, attrSize, (short) 4, 4, null, evalExpr);	
		MapMidPair mpair = fscan1.get_nextMidPair();
		List<MapMidPair> lmpair = new ArrayList<>();
		while(mpair!=null)
		{
			
			lmpair.add(mpair);
			mpair = fscan1.get_nextMidPair();
			count++;
		}
		
		if(count <=3)
			return ;
		else
		{
			int delIndex = findleastTime(lmpair);
			MID delmid = lmpair.get(delIndex).mid;			

			if(_hf1.deleteRecord(delmid));
			else if(_hf2.deleteRecord(delmid))
			{
				this._bf2 = new BTreeFile(name + "Index2", AttrType.attrString, 22, 0);
				deleteAllNodesInIndex(_bf2);
				this._bf2 = new BTreeFile(name + "Index2", AttrType.attrString, 22, 0);
				key = rowLabel;
				MID mid = new MID();
				Scan scan2 = new Scan(_hf2);
				Map temp = scan2.getNext(mid);
				while (temp != null) {
					key = temp.getRowLabel();
					_bf2.insert(new StringKey(key), mid);
					temp = scan2.getNext(mid);
				}
				scan2.closescan();
				_bf2.close();
				
			}
			else if(_hf3.deleteRecord(delmid))
			{
				this._bf3 = new BTreeFile(name + "Index3", AttrType.attrString, 22, 0);
				deleteAllNodesInIndex(_bf3);
				this._bf3 = new BTreeFile(name + "Index3", AttrType.attrString, 22, 0);
				key = colLabel;
				MID mid = new MID();
				Scan scan2 = new Scan(_hf3);
				Map temp = scan2.getNext(mid);
				while (temp != null) {
					key = temp.getColumnLabel();
					_bf3.insert(new StringKey(key), mid);
					temp = scan2.getNext(mid);
				}
				scan2.closescan();
				_bf3.close();
			}
			else if(_hf4.deleteRecord(delmid))
			{
				this._bf4 = new BTreeFile(name + "Index4", AttrType.attrString, 44, 0);
				deleteAllNodesInIndex(_bf4);
				this._bf4 = new BTreeFile(name + "Index4", AttrType.attrString, 44, 0);
				key = colLabel+rowLabel;
				MID mid = new MID();
				Scan scan2 = new Scan(_hf4);
				Map temp = scan2.getNext(mid);
				while (temp != null) {
					key = temp.getColumnLabel()+temp.getRowLabel();
					_bf4.insert(new StringKey(key), mid);
					temp = scan2.getNext(mid);
				}
				scan2.closescan();
				_bf4.close();
			}
			
			else if(_hf5.deleteRecord(delmid))
			{
				this._bf5 = new BTreeFile(name + "Index5", AttrType.attrString, 44, 0);
				deleteAllNodesInIndex(_bf5);
				this._bf5 = new BTreeFile(name + "Index5", AttrType.attrString, 44, 0);
				MID mid = new MID();
				Scan scan2 = new Scan(_hf5);
				Map temp = scan2.getNext(mid);
				while (temp != null) {
					key = temp.getRowLabel()+temp.getValue();
					_bf5.insert(new StringKey(key), mid);
					temp = scan2.getNext(mid);
				}
				scan2.closescan();
				_bf5.close();
			}
		}
		
	}


	/**
	 * Remove Duplicates to handle the versioning in the database
	 * 
	 * @throws InvalidSlotNumberException
	 * @throws HFException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws Exception
	 */
	public void removeDuplicates()
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception 
	{
		
		/**
		 * Access all the maps and find if any map count is greater than 4 and delete the appropriate file.
		 */
		iscan = new IndexScan(new IndexType(IndexType.Row_Index), name, name + "Temp", attrType, attrSize, 4, 4, null,
				expr, 1, true);
		MapMidPair mpair = iscan.get_nextMidPair();
		String key = "";
		String oldKey = "";
		if (mpair != null) {
			oldKey = mpair.indKey.split("%")[0];
		}
		List<MID> list = new ArrayList<MID>();
		List<Integer> typelist = new ArrayList<Integer>();
		while (mpair != null) 
		{
			key = mpair.indKey.split("%")[0];
			int typenum = Integer.parseInt(mpair.indKey.split("%")[2]);

			if (key.equals(oldKey)) 
			{
				list.add(mpair.mid);
				typelist.add(typenum);
			} 
			
			else 
			{
				list.clear();
				typelist.clear();
				oldKey = key;
				list.add(mpair.mid);
				typelist.add(typenum);
		}

			if (list.size() == 4) 
			{
				MID delmid = list.get(0);
				
				int filenum = typelist.get(0);
				
				list.remove(0);
				typelist.remove(0);
				
				if(filenum == 1) {
					_hf1.deleteRecord(delmid);
				}

				else if(filenum == 2) {
					_hf2.deleteRecord(delmid);
				}
				else if(filenum == 3) {
					_hf3.deleteRecord(delmid);
				}
				else if(filenum == 4) {
					_hf4.deleteRecord(delmid);
				}
				else if(filenum == 5) {
					_hf5.deleteRecord(delmid);
				}

				
			}

			mpair = iscan.get_nextMidPair();
		}
		iscan.close();
		_bftemp.destroyFile();
		
		destroyIndexFiles();
		
	}
	

/**
 * Destroy all the index files in the database	
 * @throws IteratorException
 * @throws UnpinPageException
 * @throws FreePageException
 * @throws DeleteFileEntryException
 * @throws ConstructPageException
 * @throws PinPageException
 * @throws IOException
 */
	private void destroyIndexFiles() throws IteratorException, UnpinPageException, FreePageException, DeleteFileEntryException, ConstructPageException, PinPageException, IOException {
		_bf2.destroyFile();
		_bf3.destroyFile();
		_bf4.destroyFile();
		_bf5.destroyFile();
		
	}
	
	/**
	 * Delete records in the specific index file
	 * @param index
	 * @throws PinPageException
	 * @throws KeyNotMatchException
	 * @throws IteratorException
	 * @throws IOException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws ScanIteratorException
	 * @throws ScanDeleteException
	 */
	public void deleteAllNodesInIndex(BTreeFile index) throws PinPageException, KeyNotMatchException, IteratorException, IOException, ConstructPageException, UnpinPageException, ScanIteratorException, ScanDeleteException {
		BTFileScan scan = index.new_scan(null, null);
		boolean isScanComplete = false;
		while(!isScanComplete) 
		{
			KeyDataEntry entry = scan.get_next();
			if(entry == null)
			{
				isScanComplete = true;
				break;
			}
		scan.delete_current();
		}
	}
	
	/**
	 * Find the position of the map to be inserted and accordingly updtae the file.
	 * @param type - type of heap file
	 * @param mapInsert - Map to be inserted
	 * @throws InvalidSlotNumberException
	 * @throws SpaceNotAvailableException
	 * @throws Exception
	 */

	public void mapInsertOrder(int type, Map mapInsert) throws InvalidSlotNumberException, SpaceNotAvailableException, Exception
	{
		Heapfile temp1 = new Heapfile("insTemp1");
		
		String rowLabel = mapInsert.getRowLabel();	
		String colLabel = mapInsert.getColumnLabel();
		String value = mapInsert.getValue();
	
		if(type == 2)
		{	
			deleteAllNodesInIndex(_bf2);
			Scan scan = _hf2.openScan();
			MID mid = new MID();
			MID prevMid = new MID();
			
			Map dummy = scan.getNext(mid);
			String label = "";
			
			if(dummy!=null)
				 label = dummy.getRowLabel();
			
			while(dummy!=null && label.compareTo(rowLabel)<0)
			{
				temp1.insertRecord(dummy.getMapByteArray());
				dummy = scan.getNext(mid);
				if(dummy!=null)
					 label = dummy.getRowLabel();
			}
			
			temp1.insertRecord(mapInsert.getMapByteArray());
			
			while(dummy!=null)
			{
				temp1.insertRecord(dummy.getMapByteArray());
				dummy = scan.getNext(mid);
			}
		
			scan.closescan();
			_hf2.deleteFile();
			
			this._bf2 = new BTreeFile(name + "Index2", AttrType.attrString, 22, 0);
			this._hf2 = new Heapfile(name + "2");
			
			Scan newscan = temp1.openScan();
			MID newmid = new MID();
			dummy = newscan.getNext(newmid);
			while(dummy!=null)
			{
				
				MID midIndex = _hf2.insertRecord(dummy.getMapByteArray());
				key= dummy.getRowLabel();
				_bf2.insert(new StringKey(key), midIndex);
				dummy = newscan.getNext(newmid);
			}
			
			_bf2.close();
			newscan.closescan();
			temp1.deleteFile();
		}
		
		
		else if(type == 3)
		{
			deleteAllNodesInIndex(_bf3);
			Scan scan = _hf3.openScan();
			MID mid = new MID();
			MID prevMid = new MID();
			
			Map dummy = scan.getNext(mid);
			String label = "";
			
			if(dummy!=null)
				 label = dummy.getColumnLabel();
			
			while(dummy!=null && label.compareTo(colLabel)<0)
			{
				temp1.insertRecord(dummy.getMapByteArray());
				dummy = scan.getNext(mid);
				if(dummy!=null)
					 label = dummy.getColumnLabel();
			}
			
			temp1.insertRecord(mapInsert.getMapByteArray());
			
			while(dummy!=null)
			{
				temp1.insertRecord(dummy.getMapByteArray());
				dummy = scan.getNext(mid);
			}
		
			scan.closescan();
			_hf3.deleteFile();
			
			this._bf3 = new BTreeFile(name + "Index3", AttrType.attrString, 22, 0);
			this._hf3 = new Heapfile(name + "3");
			
			Scan newscan = temp1.openScan();
			MID newmid = new MID();
			dummy = newscan.getNext(newmid);
			while(dummy!=null)
			{
				MID midIndex= _hf3.insertRecord(dummy.getMapByteArray());
				key= dummy.getColumnLabel();
				_bf3.insert(new StringKey(key), midIndex);
				dummy = newscan.getNext(newmid);
			}	
			newscan.closescan();
			temp1.deleteFile();
		}
		
		
		else if(type == 4)
		{
			deleteAllNodesInIndex(_bf4);
			Scan scan = _hf4.openScan();
			MID mid = new MID();
			MID prevMid = new MID();
			
			Map dummy = scan.getNext(mid);
			String label = "";
			
			if(dummy!=null)
				 label = dummy.getColumnLabel()+dummy.getRowLabel();
			
			while(dummy!=null && label.compareTo(colLabel+rowLabel)<0)
			{
				temp1.insertRecord(dummy.getMapByteArray());
				dummy = scan.getNext(mid);
				if(dummy!=null)
					 label = dummy.getColumnLabel()+dummy.getRowLabel();
			}
			
			temp1.insertRecord(mapInsert.getMapByteArray());
			
			while(dummy!=null)
			{
				temp1.insertRecord(dummy.getMapByteArray());
				dummy = scan.getNext(mid);
			}
		
			scan.closescan();
			_hf4.deleteFile();
			
			this._bf4 = new BTreeFile(name + "Index4", AttrType.attrString, 44, 0);
			this._hf4 = new Heapfile(name + "4");
			
			Scan newscan = temp1.openScan();
			MID newmid = new MID();
			dummy = newscan.getNext(newmid);
			while(dummy!=null)
			{
				MID midIndex = _hf4.insertRecord(dummy.getMapByteArray());
				key= dummy.getColumnLabel()+dummy.getRowLabel();
				_bf4.insert(new StringKey(key), midIndex);
				dummy = newscan.getNext(newmid);
			}	
			newscan.closescan();
			temp1.deleteFile();
	
			
		}
		
		else
		{
			deleteAllNodesInIndex(_bf5);
			Scan scan = _hf5.openScan();
			MID mid = new MID();
			MID prevMid = new MID();
			
			Map dummy = scan.getNext(mid);
			String label = "";
			
			if(dummy!=null)
				 label = dummy.getRowLabel()+dummy.getValue();
			
			while(dummy!=null && label.compareTo(rowLabel+value)<0)
			{
				temp1.insertRecord(dummy.getMapByteArray());
				dummy = scan.getNext(mid);
				if(dummy!=null)
					 label = dummy.getRowLabel()+dummy.getValue();
			}
			
			temp1.insertRecord(mapInsert.getMapByteArray());
			
			while(dummy!=null)
			{
				temp1.insertRecord(dummy.getMapByteArray());
				dummy = scan.getNext(mid);
			}
		
			scan.closescan();
			_hf5.deleteFile();
			
			this._bf5 = new BTreeFile(name + "Index5", AttrType.attrString, 44, 0);
			this._hf5 = new Heapfile(name + "5");
			
			Scan newscan = temp1.openScan();
			MID newmid = new MID();
			dummy = newscan.getNext(newmid);
			while(dummy!=null)
			{
				MID midIndex = _hf5.insertRecord(dummy.getMapByteArray());
				key= dummy.getRowLabel()+dummy.getValue();
				_bf5.insert(new StringKey(key), midIndex);
				dummy = newscan.getNext(newmid);
			}	
			newscan.closescan();
			temp1.deleteFile();
		
		}

	}
	
	//Sorting the contents of temporary heap file and inserting the maps in sorted into the corresponding heap files based on type. 
	/**
	 * Maintan the sorted order in the heap file as per the specification
	 * @param type - type of heap file
	 * @param numbuf - number of buffers
	 * @throws SortException
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws JoinsException
	 * @throws InvalidTypeException
	 * @throws InvalidSlotNumberException
	 * @throws InvalidInfoSizeException
	 * @throws SpaceNotAvailableException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws FileAlreadyDeletedException
	 * @throws IOException
	 * @throws Exception
	 */
	public void maintainSortedOrder(int type, int numbuf)
			throws SortException, UnknowAttrType, LowMemException, JoinsException, InvalidTypeException,
			InvalidSlotNumberException, InvalidInfoSizeException, SpaceNotAvailableException, HFException,
			HFBufMgrException, HFDiskMgrException, FileAlreadyDeletedException, IOException, Exception {

			TupleOrder[] order = new TupleOrder[2];
			order[0] = new TupleOrder(TupleOrder.Ascending);
			order[1] = new TupleOrder(TupleOrder.Descending);

			MID mid = new MID();

			hfscan = new FileScan("temp_new", attrType, attrSize, (short) 4, 4, null, null);

			Sort sort = null;
			
			if (type == 2) 
				sort = new Sort(attrType, (short) 4, attrSize, hfscan, 7, order[0], 22, numbuf);
			else if (type==3)
				sort = new Sort(attrType, (short) 4, attrSize, hfscan, 8, order[0], 22, numbuf);
			else if (type==4)
				sort = new Sort(attrType, (short) 4, attrSize, hfscan, 9, order[0], 44, numbuf);
			else if (type==5)
				sort = new Sort(attrType, (short) 4, attrSize, hfscan, 10, order[0], 44, numbuf);
			
			Map tuple = sort.get_next();
			
			switch(type)
			{
				case 2:
				{
					while (tuple != null) 
					{
						_hf2.insertRecord(tuple.getMapByteArray());
						tuple = sort.get_next();
					}
					break;
				}
				
				case 3:
				{
					while (tuple != null) 
					{
						_hf3.insertRecord(tuple.getMapByteArray());
						tuple = sort.get_next();
					}
					break;
				}
				
				case 4:
				{
					while (tuple != null) 
					{
						_hf4.insertRecord(tuple.getMapByteArray());
						tuple = sort.get_next();
					}
					break;
				}
				
				case 5:
				{
					while (tuple != null) 
					{
						_hf5.insertRecord(tuple.getMapByteArray());
						tuple = sort.get_next();
						
					}
					break;
				}
			}
			sort.close();
			temp.deleteFile();
	}

	
	//Copies the contents of the heap file into a temporary heap file based on its type
	/**
	 * Temporary Heap file to insert records and then sort them to store in the database.
	 * @param type
	 * @throws Exception
	 */
	public void createTempHF(int type) throws Exception {
		
		if (type == 2) 
		{
			scan = _hf2.openScan();
			MID rid = new MID();
			Map tuple1 =  scan.getNext(rid);
			
			while (tuple1!=null) 
			{
				temp.insertRecord(tuple1.getMapByteArray());
				tuple1 =  scan.getNext(rid);
			}
			_hf2.deleteFile();
			_hf2 = new Heapfile(name + "2");
		}
		
		if (type == 3) 
		{
			scan = _hf3.openScan();
			MID rid = new MID();
			Map tuple1 =  scan.getNext(rid);
			
			while (tuple1!=null) 
			{
				temp.insertRecord(tuple1.getMapByteArray());
				tuple1 =  scan.getNext(rid);
			}
			_hf3.deleteFile();
			_hf3 = new Heapfile(name + "3");
		}
		
		if (type == 4) {
			scan = _hf4.openScan();
			MID rid = new MID();
			Map tuple1 =  scan.getNext(rid);
			
			while (tuple1!=null) 
			{
				temp.insertRecord(tuple1.getMapByteArray());
				tuple1 =  scan.getNext(rid);
			}
			_hf4.deleteFile();
			_hf4 = new Heapfile(name + "4");
		}
		
		if (type == 5) {
			scan = _hf5.openScan();
			MID rid = new MID();
			Map tuple1 =  scan.getNext(rid);
			
			while (tuple1!=null) 
			{
				temp.insertRecord(tuple1.getMapByteArray());
				tuple1 =  scan.getNext(rid);
			}
			_hf5.deleteFile();
			_hf5 = new Heapfile(name + "5");
		}
	}
	
	/**
	 * Scan the bigT to return all the maps in the bigt in all the files.
	 * @throws InvalidInfoSizeException
	 * @throws IOException
	 */
	public void scanBigT() throws InvalidInfoSizeException, IOException
	{
		scan = _hf1.openScan();
		MID rid1 = new MID();
		System.out.println("Maps in heap file 1 are");
		Map tuple1 = scan.getNext(rid1);
		
		while (tuple1!=null) 
		{
			tuple1.print();
			System.out.println();
			tuple1 = scan.getNext(rid1);
		}
		
		scan = _hf2.openScan();
		MID rid2 = new MID();
		System.out.println("Maps in heap file 2 are");
		Map tuple2 = scan.getNext(rid2);
		while (tuple2!=null) 
		{
			tuple2.print();
			System.out.println();
			tuple2 = scan.getNext(rid2);
		}

		scan = _hf3.openScan();
		System.out.println("Maps in heap file 3 are");
		tuple2 = scan.getNext(rid2);
		while (tuple2!=null) 
		{
			tuple2.print();
			System.out.println();
			tuple2 = scan.getNext(rid2);
		}
		
		scan = _hf4.openScan();
		System.out.println("Maps in heap file 4 are");
		tuple2 = scan.getNext(rid2);
		while (tuple2!=null) 
		{
			tuple2.print();
			System.out.println();
			tuple2 = scan.getNext(rid2);
		}
		
		scan = _hf5.openScan();
		System.out.println("Maps in heap file 5 are");
		tuple2 = scan.getNext(rid2);
		while (tuple2!=null) 
		{
			tuple2.print();
			System.out.println();
			tuple2 = scan.getNext(rid2);
		}
	}


	/**
	 * Opens the stream of maps
	 * 
	 * @param orderType - Desired order of Results
	 * @param rowFilter - Filtering condition on row
	 * @param colFilter - Filtering condition on column
	 * @param valFilter - Filtering condition on value
	 * @param numbuf    - number of buffers allocated
	 * @return
	 * @throws Exception
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws HFException
	 * @throws InvalidSlotNumberException
	 */
	public Stream openStream(int orderType, String rowFilter, String colFilter, String valFilter, int numbuf)
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		Stream stream = new Stream(this, orderType, rowFilter, colFilter, valFilter, numbuf);
		return stream;

	}
	

}