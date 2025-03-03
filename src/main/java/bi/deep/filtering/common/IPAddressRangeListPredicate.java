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

import java.util.Arrays;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateMatch;

public class IPAddressRangeListPredicate implements DruidObjectPredicate<Object[]> {

    private final Predicate<IPBoundedRange[]> predicate;

    public IPAddressRangeListPredicate(Predicate<IPBoundedRange[]> predicate) {
        this.predicate = predicate;
    }

    public static IPAddressRangeListPredicate of(Predicate<IPBoundedRange[]> predicate) {
        return new IPAddressRangeListPredicate(predicate);
    }

    @Override
    public DruidPredicateMatch apply(@Nullable Object[] value) {
        if (value == null || value.length == 0 || !Arrays.stream(value).allMatch(String.class::isInstance)) {
            return DruidPredicateMatch.UNKNOWN;
        }
        String[] stringValues = Arrays.copyOf(value, value.length, String[].class);
        return DruidPredicateMatch.of(predicate.test(mapToIPAddresses(stringValues)));
    }

    private IPBoundedRange[] mapToIPAddresses(String[] values) {
        return Arrays.stream(values)
                .map(ipRange -> {
                    String[] parts = ipRange.split("-");
                    String lower = parts[0].trim();
                    String upper = parts.length > 1 ? parts[1].trim() : lower;
                    return new IPBoundedRange(lower, upper, false, false);
                })
                .toArray(IPBoundedRange[]::new);
    }
}
