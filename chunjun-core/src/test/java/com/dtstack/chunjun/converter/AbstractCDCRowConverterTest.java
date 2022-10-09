/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.converter;

import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class AbstractCDCRowConverterTest {
    /** Should return null when the input is null */
    @Test
    public void wrapIntoNullableInternalConverterWhenInputIsNullThenReturnNull() throws Exception {
        AbstractCDCRowConverter abstractCDCRowConverter =
                PowerMockito.mock(AbstractCDCRowConverter.class);
        IDeserializationConverter IDeserializationConverter =
                PowerMockito.mock(IDeserializationConverter.class);
        when(abstractCDCRowConverter.wrapIntoNullableInternalConverter(IDeserializationConverter))
                .thenCallRealMethod();
        assertNull(
                abstractCDCRowConverter
                        .wrapIntoNullableInternalConverter(IDeserializationConverter)
                        .deserialize(null));
    }
}
