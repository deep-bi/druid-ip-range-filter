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
package bi.deep.matching;

import bi.deep.util.IPRangeUtil;
import com.fasterxml.jackson.annotation.JsonTypeName;
import inet.ipaddr.IPAddress;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.extraction.DimExtractionFn;

@JsonTypeName("ip-range-extraction-fn")
public class IPRangeFilteredExtractionFn extends DimExtractionFn {

    public static final byte CACHE_TYPE_ID_IP_RANGE_FN = 0xF;

    private final List<IPAddress> ips;

    public IPRangeFilteredExtractionFn(Set<String> ips) {
        this.ips = IPRangeUtil.mapStringsToIps(ips);
    }

    @Nullable
    @Override
    public String apply(@Nullable String value) {
        if (NullHandling.sqlCompatible() && value == null) {
            return null;
        }
        return value == null ? StringUtils.EMPTY : IPRangeUtil.getMatchingIPs(value, ips);
    }

    @Override
    public boolean preservesOrdering() {
        return false;
    }

    @Override
    public ExtractionType getExtractionType() {
        return ExtractionType.MANY_TO_ONE;
    }

    @Override
    public byte[] getCacheKey() {
        return new byte[] {CACHE_TYPE_ID_IP_RANGE_FN};
    }
}
