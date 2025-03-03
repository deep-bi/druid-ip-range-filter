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
package bi.deep.filtering.ip.range;

import bi.deep.filtering.ip.range.impl.RangeMatchingIPFilterImpl;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.AbstractOptimizableDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.query.filter.Filter;

@JsonTypeName("ip_range_match")
public class RangeMatchingIPFilter extends AbstractOptimizableDimFilter implements DimFilter {
    private static final byte CACHE_ID = 0x53;
    private final String dimension;
    private final List<String> ips;
    private final boolean ignoreVersionMismatch;

    @JsonCreator
    public RangeMatchingIPFilter(
            @JsonProperty("dimension") String dimension,
            @JsonProperty("values") List<String> ips,
            @JsonProperty("ignoreVersionMismatch") @Nullable Boolean ignoreVersionMismatch) {
        this.dimension = Preconditions.checkNotNull(dimension, "dimension");
        if (ips == null || ips.isEmpty()) {
            throw new IllegalArgumentException("values are not defined");
        }
        this.ips = ips;
        this.ignoreVersionMismatch = ignoreVersionMismatch != null && ignoreVersionMismatch;
    }

    @JsonProperty("dimension")
    public String getDimension() {
        return dimension;
    }

    @JsonProperty("values")
    public List<String> getIps() {
        return ips;
    }

    @JsonProperty("ignoreVersionMismatch")
    public boolean isIgnoreVersionMismatch() {
        return ignoreVersionMismatch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RangeMatchingIPFilter)) {
            return false;
        }

        final RangeMatchingIPFilter that = (RangeMatchingIPFilter) o;

        return ignoreVersionMismatch == that.ignoreVersionMismatch
                && Objects.equals(dimension, that.dimension)
                && Objects.equals(ips, that.ips);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimension, ips, ignoreVersionMismatch);
    }

    @Override
    public Filter toFilter() {
        return new RangeMatchingIPFilterImpl(dimension, mapStringsToIps(ips), ignoreVersionMismatch);
    }

    @Nullable
    @Override
    public RangeSet<String> getDimensionRangeSet(String dimension) {
        return null;
    }

    @Override
    public Set<String> getRequiredColumns() {
        return ImmutableSet.of(dimension);
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(CACHE_ID)
                .appendString(dimension)
                .appendByte(DimFilterUtils.STRING_SEPARATOR)
                .appendString(ips.toString())
                .appendByte(DimFilterUtils.STRING_SEPARATOR)
                .appendBoolean(ignoreVersionMismatch)
                .build();
    }

    private List<IPAddress> mapStringsToIps(final List<String> ips) {
        return ips.stream().map(ip -> new IPAddressString(ip).getAddress()).collect(Collectors.toList());
    }
}
