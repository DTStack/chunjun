package com.dtstack.flinkx.rdb;

import java.io.Serializable;

public interface ParameterValuesProvider  {
    Serializable[][] getParameterValues();
}
