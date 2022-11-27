package edu.utdallas.davisbase.db.storage_engine.a_io.data;

/**
 * An identifier for a record within a file.
 * A RID consists of the block number in the file,
 * and the location of the record in that block.
 * @author Edward Sciore
 */
public class RecordId {
	private int blknum;
	private int slot;

	/**
	 * Create a RID for the record having the
	 * specified location in the specified block.
	 * @param blknum the block number where the record lives
	 * @param slot the record's loction
	 */
	public RecordId(int blknum, int slot) {
		this.blknum = blknum;
		this.slot   = slot;
	}

	/**
	 * Return the block number associated with this RID.
	 * @return the block number
	 */
	public int blockNumber() {
		return blknum;
	}

	/**
	 * Return the slot associated with this RID.
	 * @return the slot
	 */
	public int slot() {
		return slot;
	}

	public boolean equals(Object obj) {
		RecordId r = (RecordId) obj;
		return blknum == r.blknum && slot==r.slot;
	}

	public String toString() {
		return "[" + blknum + ", " + slot + "]";
	}
}
