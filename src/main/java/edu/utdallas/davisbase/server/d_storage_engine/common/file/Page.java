package edu.utdallas.davisbase.server.d_storage_engine.common.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * The Page is an in-memory Block (saved in Storage)
 *
 * @author Edward Sciore
 */
public class Page {
    public static Charset CHARSET = StandardCharsets.US_ASCII;
    private ByteBuffer bb;

    // For creating data buffers
    public Page(int blocksize) {
        bb = ByteBuffer.allocateDirect(blocksize);
    }

    public static int maxBytesRequiredForString(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int) bytesPerChar);
    }

    public int getInt(int offset) {
        return bb.getInt(offset);
    }

    public void setInt(int offset, int n) {
        bb.putInt(offset, n);
    }

    public byte[] getBytes(int offset) {
        bb.position(offset);
        int length = bb.getInt();
        byte[] b = new byte[length];
        bb.get(b);
        return b;
    }

    public void setBytes(int offset, byte[] b) {
        bb.position(offset);
        bb.putInt(b.length);
        bb.put(b);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public void setString(int offset, String s) {
        byte[] b = s.getBytes(CHARSET);
        setBytes(offset, b);
    }

    // a package private method, needed by FileMgr
    ByteBuffer contents() {
        bb.position(0);
        return bb;
    }
}
