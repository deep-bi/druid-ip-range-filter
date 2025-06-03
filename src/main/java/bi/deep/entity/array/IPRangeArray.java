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

import bi.deep.entity.SerializationUtil;
import bi.deep.util.IPRangeUtil;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import inet.ipaddr.IPAddress;
import inet.ipaddr.format.IPAddressRange;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.druid.java.util.common.IAE;

@JsonSerialize(using = IPRangeArraySerializer.class)
public class IPRangeArray implements Serializable, Comparable<IPRangeArray> {
    public static final IPRangeArray EMPTY = new IPRangeArray(Collections.emptyList());
    public static final Comparator<IPRangeArray> COMPARATOR = Comparator.nullsFirst(IPRangeArray::compareTo);
    private final List<IPAddressRange> addressRanges;

    public IPRangeArray(@Nullable List<IPAddressRange> addressRanges) {
        this.addressRanges = addressRanges;
    }

    public static IPRangeArray fromArray(List<Object> values) {
        if (CollectionUtils.isEmpty(values)) {
            return EMPTY;
        }

        return new IPRangeArray(values.stream()
                .map(Objects::toString)
                .map(IPRangeUtil::fromString)
                .collect(Collectors.toList()));
    }

    public List<IPAddressRange> getAddressRanges() {
        return addressRanges;
    }

    public boolean match(IPAddress value) {
        return CollectionUtils.isNotEmpty(addressRanges)
                && addressRanges.stream().anyMatch(m -> m.contains(value));
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
        int minSize = Math.min(addressRanges.size(), other.getAddressRanges().size());

        for (int i = 0; i < minSize; i++) {
            int cmp = addressRanges.get(i).compareTo(other.getAddressRanges().get(i));
            if (cmp != 0) {
                return cmp;
            }
        }

        return Integer.compare(addressRanges.size(), other.getAddressRanges().size());
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
