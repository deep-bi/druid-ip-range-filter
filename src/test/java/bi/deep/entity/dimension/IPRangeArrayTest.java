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
package bi.deep.entity.dimension;

import static inet.ipaddr.Address.ADDRESS_LOW_VALUE_COMPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.format.IPAddressRange;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IPRangeArrayTest {
    @Test
    void testIP4Creation() {
        final IPRangeArray ipV4Range = IPRangeArray.fromArray(ImmutableList.of("0.0.0.0/255.255.255.255"));
        assertNotNull(ipV4Range);

        Set<IPAddressRange> addressRanges = ipV4Range.getAddressRanges();
        assertEquals(1, addressRanges.size());

        IPAddressRange range = addressRanges.iterator().next();
        assertEquals("0.0.0.0", range.getLower().toString());
        assertEquals("255.255.255.255", range.getUpper().toString());

        assertFalse(range.getLower().isIPv6());
        assertTrue(range.getLower().isIPv4());
    }

    @Test
    void testIP6Creation() {
        final IPRangeArray ipV6Range = IPRangeArray.fromArray(
                ImmutableList.of("6f:ad2f:938:5f8f:7f94:ddd0:e1a5:4f/58db:b320:7914:41f9:12ca:5ccc:9c05:cd1e"));
        assertNotNull(ipV6Range);

        Set<IPAddressRange> addressRanges = ipV6Range.getAddressRanges();
        assertEquals(1, addressRanges.size());

        IPAddressRange range = addressRanges.iterator().next();
        assertEquals("6f:ad2f:938:5f8f:7f94:ddd0:e1a5:4f", range.getLower().toString());
        assertEquals("58db:b320:7914:41f9:12ca:5ccc:9c05:cd1e", range.getUpper().toString());

        assertTrue(range.getLower().isIPv6());
        assertFalse(range.getLower().isIPv4());
    }

    @ParameterizedTest
    @ValueSource(strings = {"39.181.2.192", "6f:ad2f:938:5f8f:7f94:ddd0:e1a5:4f"})
    void rangeContainsTest(String addressStr) {
        final long count = 10;
        final IPAddress refAddress = new IPAddressString(addressStr).getAddress();
        final IPAddress lower = refAddress.increment(count);
        final IPAddress upper = lower.increment(count);
        final IPRangeArray ipRangeArray =
                IPRangeArray.fromArray(ImmutableList.of(String.format("%s-%s", lower, upper)));

        assertNotNull(ipRangeArray);
        assertTrue(ipRangeArray.contains(lower));
        assertTrue(ipRangeArray.contains(upper));

        // Within Range
        assertTrue(LongStream.range(0, count).mapToObj(lower::increment).allMatch(ipRangeArray::contains));
        // Lower than Range
        assertTrue(LongStream.range(0, count).mapToObj(refAddress::increment).noneMatch(ipRangeArray::contains));
        // More than Range
        assertTrue(LongStream.range(1, count).mapToObj(upper::increment).noneMatch(ipRangeArray::contains));
    }

    @ParameterizedTest
    @ValueSource(strings = {"39.181.2.192", "6f:ad2f:938:5f8f:7f94:ddd0:e1a5:4f"})
    void rangeContainsTestList(String addressStr) {
        final long count = 10;
        final IPAddress refAddress = new IPAddressString(addressStr).getAddress();
        final IPAddress lower = refAddress.increment(count);
        final IPAddress upper = lower.increment(count);
        final IPRangeArray ipRangeArray =
                IPRangeArray.fromArray(ImmutableList.of(String.format("%s-%s", lower, upper)));

        assertNotNull(ipRangeArray);
        assertTrue(ipRangeArray.contains(lower));
        assertTrue(ipRangeArray.contains(upper));

        // Within Range
        assertTrue(LongStream.range(0, count).mapToObj(lower::increment).allMatch(ipRangeArray::contains));
        // Lower than Range
        assertTrue(LongStream.range(0, count).mapToObj(refAddress::increment).noneMatch(ipRangeArray::contains));
        // More than Range
        assertTrue(LongStream.range(1, count).mapToObj(upper::increment).noneMatch(ipRangeArray::contains));

        SortedSet<IPAddress> searchSet = new TreeSet<>(ADDRESS_LOW_VALUE_COMPARATOR);
        searchSet.add(refAddress.increment(count - 1));
        searchSet.add(upper.increment(1));

        assertFalse(ipRangeArray.contains(searchSet));
    }
}
