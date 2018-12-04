package com.dtstack.flinkx.carbondata.writer;


public interface GenericParser {

    void addChild(GenericParser child);

    void parseString(String input);

}
