package com.dtstack.flinkx.connector.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public interface DataProcessor<T> {

    List<T> process(ServiceProcessor.Context entity) throws IOException, SQLException;

    boolean moreData();

    List<T> processException(Exception e);
}
