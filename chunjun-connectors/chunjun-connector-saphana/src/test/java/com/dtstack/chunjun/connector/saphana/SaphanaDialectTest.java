package com.dtstack.chunjun.connector.saphana;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class SaphanaDialectTest {

    public static void main(String[] args) {
        byte[] a = short2byte(Short.valueOf("220"));
        System.out.println(byte2short(a));
        byte[] b = short2byte(Short.valueOf("120"));
        System.out.println(byte2short(b));
    }

    public static byte[] short2byte(short s) {
        byte[] b = new byte[2];
        for (int i = 0; i < 2; i++) {
            int offset = 16 - (i + 1) * 8; // 因为byte占4个字节，所以要计算偏移量
            b[i] = (byte) ((s >> offset) & 0xff); // 把16位分为2个8位进行分别存储
        }
        return b;
    }

    public static short byte2short(byte[] b) {
        short l = 0;
        for (int i = 0; i < 2; i++) {
            l <<= 8; // <<=和我们的 +=是一样的，意思就是 l = l << 8
            l |= (b[i] & 0xff); // 和上面也是一样的  l = l | (b[i]&0xff)
        }
        return l;
    }
}
