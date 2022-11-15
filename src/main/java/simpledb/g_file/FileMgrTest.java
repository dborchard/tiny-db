package simpledb.g_file;


import simpledb.a_server.SimpleDB;

import java.io.IOException;

public class FileMgrTest {
    public static void main(String[] args) throws IOException {
        SimpleDB db = new SimpleDB("filetest");
        FileMgr fm = db.fileMgr();
        BlockId blk = new BlockId("testfile", 2);
        int pos1 = 88;

        Page p1 = new Page(fm.blockSize());
        p1.setString(pos1, "abcdefghijklm");
        int size = Page.maxBytesRequiredForString("abcdefghijklm".length());
        int pos2 = pos1 + size;
        p1.setInt(pos2, 345);
        fm.write(blk, p1);

        Page p2 = new Page(fm.blockSize());
        fm.read(blk, p2);
        System.out.println("offset " + pos2 + " contains " + p2.getInt(pos2));
        System.out.println("offset " + pos1 + " contains " + p2.getString(pos1));


        p1 = new Page(fm.blockSize());
//        p1.setString(pos1, "arjun");
        p1.setInt(pos2, 100);
        fm.write(blk, p1);

        p2 = new Page(fm.blockSize());
        fm.read(blk, p2);
        System.out.println("offset " + pos2 + " contains " + p2.getInt(pos2));
        System.out.println("offset " + pos1 + " contains " + p2.getString(pos1));
    }
}