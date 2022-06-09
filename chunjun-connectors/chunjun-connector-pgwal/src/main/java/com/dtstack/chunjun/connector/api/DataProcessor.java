package com.dtstack.chunjun.connector.api;

import java.util.List;

public interface DataProcessor<T> {

    List<T> process(ServiceProcessor.Context entity) throws Exception;

    boolean moreData();

    List<T> processException(Exception e);
}
