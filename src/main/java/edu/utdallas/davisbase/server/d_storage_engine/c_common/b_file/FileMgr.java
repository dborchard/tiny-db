package edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileMgr {
    private final File dbDirectory;
    private final int blockSize;
    private final boolean isNew;

    public FileMgr(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;

        isNew = !dbDirectory.exists();

        // create the directory if the database is new
        if (isNew) dbDirectory.mkdirs();
    }

    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getRandomAccessFile(blk.fileName());
            f.seek((long) blk.number() * blockSize);
            f.getChannel().read(p.contents());
            f.close();
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }


    public synchronized void write(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getRandomAccessFile(blk.fileName());
            f.seek((long) blk.number() * blockSize);
            f.getChannel().write(p.contents());
            f.close();
        } catch (IOException e) {
            throw new RuntimeException("cannot write block" + blk);
        }
    }


    public synchronized BlockId append(String filename) {
        int newBlockNumber = length(filename);
        BlockId blk = new BlockId(filename, newBlockNumber);
        byte[] b = new byte[blockSize];
        try {
            RandomAccessFile f = getRandomAccessFile(blk.fileName());
            f.seek((long) blk.number() * blockSize);
            f.write(b);
            f.close();
        } catch (IOException e) {
            throw new RuntimeException("cannot append block" + blk);
        }
        return blk;
    }


    public int length(String filename) {
        try {
            RandomAccessFile f = getRandomAccessFile(filename);
            int result = (int) (f.length() / blockSize);
            f.close();
            return result;
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }


    public boolean isNew() {
        return isNew;
    }


    public int blockSize() {
        return blockSize;
    }

    private RandomAccessFile getRandomAccessFile(String fileName) throws FileNotFoundException {
        File dbTable = new File(dbDirectory, fileName);
        return new RandomAccessFile(dbTable, "rws");
    }

}
