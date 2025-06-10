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
package bi.deep.entity.array;

import static inet.ipaddr.Address.ADDRESS_LOW_VALUE_COMPARATOR;

import bi.deep.entity.SerializationUtil;
import bi.deep.util.IPRangeUtil;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import inet.ipaddr.IPAddress;
import inet.ipaddr.format.IPAddressRange;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.druid.java.util.common.IAE;

@JsonSerialize(using = IPRangeArraySerializer.class)
public class IPRangeArray implements Serializable, Comparable<IPRangeArray> {
    public static final IPRangeArray EMPTY = new IPRangeArray(Collections.emptySortedSet());
    public static final Comparator<IPRangeArray> COMPARATOR = Comparator.nullsFirst(IPRangeArray::compareTo);
    private final SortedSet<IPAddressRange> addressRanges;

    public IPRangeArray(SortedSet<IPAddressRange> addressRanges) {
        this.addressRanges = addressRanges;
    }

    public static IPRangeArray fromArray(List<Object> values) {
        if (CollectionUtils.isEmpty(values)) {
            return EMPTY;
        }

        return new IPRangeArray(values.stream()
                .map(Objects::toString)
                .map(IPRangeUtil::fromString)
                .collect(Collectors.toCollection(() -> new TreeSet<>(ADDRESS_LOW_VALUE_COMPARATOR))));
    }

    public Set<IPAddressRange> getAddressRanges() {
        return addressRanges;
    }

    public boolean match(IPAddress address) {
        for (IPAddressRange range : addressRanges) {
            if (range.getLower().compareTo(address) > 0) {
                return false;
            }

            if (range.contains(address)) {
                return true;
            }
        }

        return false;
    }

    public boolean match(SortedSet<IPAddress> addresses) {
        Iterator<IPAddressRange> rangeIter = addressRanges.iterator();
        Iterator<IPAddress> addressIter = addresses.iterator();

        if (!rangeIter.hasNext() || !addressIter.hasNext()) {
            return false; // One set is empty
        }

        IPAddressRange range = rangeIter.next();
        IPAddress address = addressIter.next();

        while (true) {
            int cmp = address.compareTo(range.getLower());

            if (cmp < 0) {
                if (addressIter.hasNext()) {
                    address = addressIter.next();
                } else {
                    break;
                }
            } else {
                if (range.contains(address)) {
                    return true;
                }

                if (rangeIter.hasNext()) {
                    range = rangeIter.next();
                } else {
                    break;
                }
            }
        }

        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof IPRangeArray)) {
            return false;
        }

        final IPRangeArray that = (IPRangeArray) obj;
        return Objects.equals(addressRanges, that.addressRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(addressRanges);
    }

    @Override
    public String toString() {
        return Objects.toString(addressRanges);
    }

    @Override
    public int compareTo(IPRangeArray other) {
        Iterator<IPAddressRange> iter = addressRanges.iterator();
        Iterator<IPAddressRange> otherIter = other.addressRanges.iterator();

        while (iter.hasNext() && otherIter.hasNext()) {
            int cmp = ADDRESS_LOW_VALUE_COMPARATOR.compare(iter.next(), otherIter.next());

            if (cmp != 0) {
                return cmp;
            }
        }

        if (iter.hasNext()) return 1;
        if (otherIter.hasNext()) return -1;
        return 0;
    }

    public byte[] toBytes() {
        try {
            return SerializationUtil.serialize(this);
        } catch (IOException e) {
            throw new IAE("Unable to covert to bytes", e);
        }
    }

    public static IPRangeArray from(Object input) {
        if (input == null) {
            return EMPTY;
        }
        if (input instanceof IPRangeArray) {
            return (IPRangeArray) input;
        }

        if (input instanceof List) {
            return IPRangeArray.fromArray((List<Object>) input);
        }

        try {
            return SerializationUtil.deserialize((byte[]) input, IPRangeArray.class);
        } catch (Exception e) {
            throw new IAE("Unable to read input", e);
        }
    }
}
