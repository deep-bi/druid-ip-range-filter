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
package bi.deep.filtering.ip.range.impl;

import bi.deep.filtering.common.IPAddressPredicate;
import bi.deep.filtering.common.IPAddressPredicateFactory;
import bi.deep.filtering.common.IPBoundedRange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import inet.ipaddr.IPAddress;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.index.BitmapColumnIndex;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Objects;
import java.util.Set;

public class SingleTypeIPRangeFilterImpl implements Filter {
    private final String column;
    private final IPBoundedRange boundedRange;
    private final boolean ignoreVersionMismatch;

    public SingleTypeIPRangeFilterImpl(String column, IPBoundedRange range, boolean ignoreVersionMismatch) {
        if (column == null) {
            throw InvalidInput.exception("Column cannot be null");
        }

        this.column = column;
        this.ignoreVersionMismatch = ignoreVersionMismatch;
        this.boundedRange = range;
    }

    @Nullable
    @Override
    public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector columnIndexSelector) {
        return null;
    }

    @Override
    public ValueMatcher makeMatcher(ColumnSelectorFactory factory) {
        return factory.makeDimensionSelector(new DefaultDimensionSpec(column, column))
                      .makeValueMatcher(new IPAddressPredicateFactory(IPAddressPredicate.of(this::contains)));
    }

    @VisibleForTesting
    public boolean contains(@NotNull final IPAddress ipAddress) {
        return boundedRange.contains(ipAddress, ignoreVersionMismatch);
    }

    @Override
    public Set<String> getRequiredColumns() {
        return ImmutableSet.of(column);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SingleTypeIPRangeFilterImpl)) {
            return false;
        }

        final SingleTypeIPRangeFilterImpl that = (SingleTypeIPRangeFilterImpl) o;

        return ignoreVersionMismatch == that.ignoreVersionMismatch
                && Objects.equals(column, that.column)
                && Objects.equals(boundedRange, that.boundedRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, boundedRange, ignoreVersionMismatch);
    }
}
