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
package bi.deep.entity;

import bi.deep.util.IPRangeUtil;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import inet.ipaddr.IPAddress;
import inet.ipaddr.format.IPAddressRange;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.druid.java.util.common.IAE;

@JsonSerialize(using = IPRangeSerializer.class)
public class IPRange implements Serializable, Comparable<IPRange> {
    public static final IPRange EMPTY = new IPRange(null);
    public static final Comparator<IPRange> COMPARATOR = Comparator.nullsFirst(IPRange::compareTo);
    private final IPAddressRange addressRange;

    public IPRange(@Nullable IPAddressRange addressRange) {
        this.addressRange = addressRange;
    }

    public static IPRange fromString(String val) {
        return new IPRange(IPRangeUtil.fromString(val));
    }

    public IPAddressRange getAddressRange() {
        return addressRange;
    }

    public boolean match(IPAddress value) {
        if (value == null) {
            return false;
        }

        if (addressRange != null) {
            return addressRange.contains(value);
        }

        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof IPRange)) {
            return false;
        }

        final IPRange that = (IPRange) obj;
        return Objects.equals(addressRange, that.addressRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(addressRange);
    }

    @Override
    public String toString() {
        return Objects.toString(addressRange);
    }

    @Override
    public int compareTo(IPRange other) {
        return getAddressRange().compareTo(other.getAddressRange());
    }

    public byte[] toBytes() {
        try {
            return SerializationUtil.serialize(this);
        } catch (IOException e) {
            throw new IAE("Unable to covert to bytes", e);
        }
    }

    public static IPRange from(Object input) {
        if (input == null) {
            return EMPTY;
        }
        if (input instanceof IPRange) {
            return (IPRange) input;
        }

        if (input instanceof String) {
            return IPRange.fromString((String) input);
        }

        try {
            return SerializationUtil.deserialize((byte[]) input, IPRange.class);
        } catch (Exception e) {
            throw new IAE("Unable to read input", e);
        }
    }
}
