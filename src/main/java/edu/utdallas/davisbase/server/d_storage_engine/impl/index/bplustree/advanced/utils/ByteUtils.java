package edu.utdallas.davisbase.server.d_storage_engine.impl.index.bplustree.advanced.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * ByteUtils for SerDe Java Object.
 * <p>
 * Ref: https://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array
 *
 * @author Arjun Sunil Kumar
 */
public class ByteUtils {

    public static byte[] convertToBytes(Object object) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            return bos.toByteArray();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static Object convertFromBytes(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes); ObjectInputStream in = new ObjectInputStream(bis)) {
            return in.readObject();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }


}
