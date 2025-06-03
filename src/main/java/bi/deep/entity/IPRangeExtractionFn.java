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
package bi.deep.entity;

import com.fasterxml.jackson.annotation.JsonTypeName;
import javax.annotation.Nullable;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.extraction.DimExtractionFn;

@JsonTypeName("ip-range-fn")
public class IPRangeExtractionFn extends DimExtractionFn {
    public static final byte CACHE_TYPE_ID_IP_RANGE_FN = 0x7F;
    private final String range;

    public IPRangeExtractionFn(String ips) {
        this.range = ips;
    }

    @Nullable
    @Override
    public String apply(@Nullable String value) {
        return null;
    }

    @Nullable
    @Override
    public String apply(@Nullable Object input) {
        if (NullHandling.sqlCompatible() && input == null) {
            return null;
        }

        if (input instanceof IPRange) {
            IPRange range = (IPRange) input;
            return range.toString();
        }

        return null;
    }

    @Override
    public boolean preservesOrdering() {
        return false;
    }

    @Override
    public ExtractionType getExtractionType() {
        return ExtractionType.ONE_TO_ONE;
    }

    @Override
    public byte[] getCacheKey() {
        return new byte[] {CACHE_TYPE_ID_IP_RANGE_FN};
    }
}
