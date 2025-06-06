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

import bi.deep.guice.IPAddressDimensionModule;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import java.util.Set;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;

@JsonTypeName("ip-range-array-filtered-spec")
public class IPRangeArrayFilteredDimensionSpec implements DimensionSpec {
    public static final byte CACHE_TYPE_ID_IP_RANGE_DIM = 0x6F;
    private final String name;
    private final DimensionSpec delegate;
    private final Set<String> ips;

    @JsonCreator
    public IPRangeArrayFilteredDimensionSpec(
            @JsonProperty("name") String name,
            @JsonProperty("delegate") DimensionSpec delegate,
            @JsonProperty("values") Set<String> ips) {
        if (ips == null || ips.isEmpty()) {
            throw new IllegalArgumentException("values are not defined");
        }
        this.name = Preconditions.checkNotNull(name, "name must be defined");
        this.delegate = Preconditions.checkNotNull(delegate, "delegate");
        this.ips = ips;
    }

    @JsonProperty("delegate")
    public DimensionSpec getDelegate() {
        return delegate;
    }

    @JsonProperty("values")
    public Set<String> getIps() {
        return ips;
    }

    @Override
    public String getDimension() {
        return delegate.getDimension();
    }

    @Override
    @JsonProperty("name")
    public String getOutputName() {
        return name;
    }

    @Override
    public ColumnType getOutputType() {
        return IPAddressDimensionModule.ARRAY_TYPE;
    }

    @Override
    public ExtractionFn getExtractionFn() {
        return null;
    }

    @Override
    public DimensionSelector decorate(DimensionSelector selector) {
        return selector;
    }

    @Override
    public boolean mustDecorate() {
        return false;
    }

    @Override
    public boolean preservesOrdering() {
        return true;
    }

    @Override
    public DimensionSpec withDimension(String newDimension) {
        return new IPRangeArrayFilteredDimensionSpec(name, getDelegate().withDimension(newDimension), ips);
    }

    @Override
    public byte[] getCacheKey() {
        return new byte[] {CACHE_TYPE_ID_IP_RANGE_DIM};
    }
}
