package com.dtstack.flinkx.pgwal;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;

public class PgDecoderTest extends TestCase {

    PgDecoder decoder = new PgDecoder(new HashMap<>());

    public void testUnquoteIdentifierPart() {
    }

    public void testDecode() throws SQLException {
        ByteBuffer buf = ByteBuffer.allocate(1000);
        buf.put((byte) 'B'); //begin id
        buf.putLong(1L); // tid
        buf.putLong(1000L); //timestamp
        buf.putInt(1); // xid
        buf.rewind();
        Table begin = decoder.decode(buf);
        assertEquals(PgMessageTypeEnum.BEGIN, begin.getType());
    }
}