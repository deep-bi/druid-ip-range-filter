/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package bi.deep.guice;

import bi.deep.entity.dimension.IPRangeArrayDimensionHandler;
import bi.deep.entity.dimension.IPRangeArrayDimensionSchema;
import bi.deep.entity.dimension.IPRangeArraySerde;
import bi.deep.entity.dimension.IPRangeDimensionHandler;
import bi.deep.entity.dimension.IPRangeDimensionSchema;
import bi.deep.entity.dimension.IPRangeSerde;
import bi.deep.filtering.ip.range.IPRangeMatchingFilter;
import bi.deep.matching.IPRangeArrayFilteredDimensionSpec;
import bi.deep.matching.IPRangeArrayFilteredVirtualColumn;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import java.util.Collections;
import java.util.List;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetrics;

public class IPRangeDimensionModule implements DruidModule {
    public static final String TYPE_NAME = "ipRange";
    public static final String ARRAY_TYPE_NAME = "ipRangeArray";
    public static final ColumnType TYPE = ColumnType.ofComplex(TYPE_NAME);
    public static final ColumnType ARRAY_TYPE = ColumnType.ofComplex(ARRAY_TYPE_NAME);

    @Override
    public void configure(Binder binder) {
        registerSerde();
    }

    @Override
    public List<? extends Module> getJacksonModules() {
        return Collections.singletonList(new SimpleModule(getClass().getSimpleName())
                .registerSubtypes(IPRangeDimensionSchema.class)
                .registerSubtypes(IPRangeArrayDimensionSchema.class)
                .registerSubtypes(IPRangeMatchingFilter.class)
                .registerSubtypes(IPRangeArrayFilteredDimensionSpec.class)
                .registerSubtypes(IPRangeArrayFilteredVirtualColumn.class));
    }

    @VisibleForTesting
    public static void registerSerde() {
        ComplexMetrics.registerSerde(TYPE_NAME, new IPRangeSerde());
        DimensionHandlerUtils.registerDimensionHandlerProvider(TYPE_NAME, IPRangeDimensionHandler::new);

        ComplexMetrics.registerSerde(ARRAY_TYPE_NAME, new IPRangeArraySerde());
        DimensionHandlerUtils.registerDimensionHandlerProvider(ARRAY_TYPE_NAME, IPRangeArrayDimensionHandler::new);
    }
}
