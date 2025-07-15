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
package bi.deep.filtering.common;

import static inet.ipaddr.Address.ADDRESS_LOW_VALUE_COMPARATOR;

import bi.deep.entity.dimension.IPRange;
import bi.deep.entity.dimension.IPRangeArray;
import inet.ipaddr.IPAddress;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;

public class MatchPredicateFactory implements DruidPredicateFactory {
    private final SortedSet<IPAddress> ips = new TreeSet<>(ADDRESS_LOW_VALUE_COMPARATOR);

    public MatchPredicateFactory(List<IPAddress> ips) {
        this.ips.addAll(ips);
    }

    @Override
    public DruidObjectPredicate<String> makeStringPredicate() {
        return null;
    }

    @Override
    public DruidLongPredicate makeLongPredicate() {
        return null;
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate() {
        return null;
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate() {
        return null;
    }

    @Override
    public DruidObjectPredicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> inputType) {
        return null;
    }

    @Override
    public DruidObjectPredicate<Object> makeObjectPredicate() {
        return object -> {
            if (object instanceof IPRange) {
                IPRange ipRange = (IPRange) object;
                return DruidPredicateMatch.of(ipRange.contains(ips));
            }
            if (object instanceof IPRangeArray) {
                IPRangeArray ipRange = (IPRangeArray) object;
                return DruidPredicateMatch.of(ipRange.contains(ips));
            }

            return DruidPredicateMatch.of(false);
        };
    }
}
