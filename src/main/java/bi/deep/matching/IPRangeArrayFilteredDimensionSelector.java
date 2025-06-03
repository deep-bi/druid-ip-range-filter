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

import bi.deep.entity.array.IPRangeArray;
import inet.ipaddr.IPAddress;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;

public class IPRangeArrayFilteredDimensionSelector extends AbstractDimensionSelector {
    protected final ColumnValueSelector<IPRangeArray> columnSelector;
    private final List<IPAddress> rangesToMatch;

    public IPRangeArrayFilteredDimensionSelector(
            ColumnValueSelector<IPRangeArray> columnSelector, List<IPAddress> rangesToMatch) {
        this.columnSelector = columnSelector;
        this.rangesToMatch = rangesToMatch;
    }

    @Override
    @Nonnull
    public IndexedInts getRow() {
        return ZeroIndexedInts.instance();
    }

    @Override
    @Nonnull
    public ValueMatcher makeValueMatcher(@Nullable String value) {
        throw new UnsupportedOperationException(value);
    }

    @Override
    @Nonnull
    public ValueMatcher makeValueMatcher(@Nonnull DruidPredicateFactory predicateFactory) {
        throw new UnsupportedOperationException("With Predicates");
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
        columnSelector.inspectRuntimeShape(inspector);
    }

    @Nullable
    @Override
    public String getObject() {
        IPRangeArray value = columnSelector.getObject();

        if (NullHandling.sqlCompatible() && value == null) {
            return null;
        }

        if (value == null) {
            return StringUtils.EMPTY;
        }

        return rangesToMatch.stream().anyMatch(value::match) ? value.toString() : StringUtils.EMPTY;
    }

    @Override
    @Nonnull
    public Class<?> classOfObject() {
        return Object.class;
    }

    @Override
    public int getValueCardinality() {
        return CARDINALITY_UNKNOWN;
    }

    @Nullable
    @Override
    public String lookupName(int id) {
        return getObject();
    }

    @Override
    public boolean nameLookupPossibleInAdvance() {
        return false;
    }

    @Nullable
    @Override
    public IdLookup idLookup() {
        return null;
    }
}
