package edu.utdallas.davisbase.server.d_storage_engine.impl.index.bplustree.utils;

import java.io.*;

public class ByteUtils {

    public static byte[] convertToBytes(Object object)  {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            return bos.toByteArray();
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }

    public static Object convertFromBytes(byte[] bytes)  {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes); ObjectInputStream in = new ObjectInputStream(bis)) {
            return in.readObject();
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }


}
