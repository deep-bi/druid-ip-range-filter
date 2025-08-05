/*
 * Copyright Deep BI, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bi.deep.range;

import com.fasterxml.jackson.annotation.JsonCreator;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.format.IPAddressRange;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;

public class IPRange {
    private static final String SEPARATOR = "/";
    private final IPAddressRange addressRange;
    private final IPAddress.IPVersion ipVersion;

    @JsonCreator
    public IPRange(String range) {
        if (StringUtils.isBlank(range)) {
            throw InvalidInput.exception("Range cannot be null or empty");
        }

        range = range.trim();

        IPAddress lower;
        IPAddress upper;

        if (range.contains(SEPARATOR)) {
            String[] parts = range.split(SEPARATOR, 2);
            if (parts.length != 2) {
                throw InvalidInput.exception("Malformed range '%s'. Expected ip/prefix (CIDR) or lower/upper.", range);
            }
            if (parts[0].isEmpty() || parts[1].isEmpty()) {
                throw InvalidInput.exception("Malformed range '%s'. Expected ip/prefix (CIDR) or lower/upper.", range);
            }
            if (StringUtils.isNumeric(parts[1])) { // CIDR
                IPAddress cidr = new IPAddressString(range).getAddress();
                if (cidr == null) {
                    throw InvalidInput.exception("Malformed CIDR '%s'. Expected ip/prefix.", range);
                }
                IPAddress block = cidr.toPrefixBlock();
                lower = block.getLower();
                upper = block.getUpper();
            } else { // lower/upper
                lower = new IPAddressString(parts[0]).getAddress();
                upper = new IPAddressString(parts[1]).getAddress();
            }
        } else {
            lower = new IPAddressString(range).getAddress();
            if (lower == null) {
                throw InvalidInput.exception(
                        "Malformed input '%s'. Expected IP address, ip/prefix (CIDR) or lower/upper.", range);
            }
            upper = lower;
        }

        if (lower == null) {
            throw InvalidInput.exception("Invalid lower IP '%s'.", range);
        }

        if (upper == null) {
            throw InvalidInput.exception("Invalid upper IP '%s'.", range);
        }

        if (!lower.getIPVersion().equals(upper.getIPVersion())) {
            throw new IAE("IPv4/IPv6 mismatch: '%s' vs '%s'.", lower, upper);
        }

        this.addressRange = lower.spanWithRange(upper);
        this.ipVersion = lower.getIPVersion();
    }

    public boolean contains(final IPAddress address) {
        return addressRange.contains(address);
    }

    public boolean isIPv4() {
        return this.ipVersion == IPAddress.IPVersion.IPV4;
    }

    public boolean isIPv6() {
        return this.ipVersion == IPAddress.IPVersion.IPV6;
    }

    public IPAddress getLower() {
        return addressRange.getLower();
    }

    public IPAddress getUpper() {
        return addressRange.getUpper();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IPRange)) {
            return false;
        }

        final IPRange that = (IPRange) o;
        return Objects.equals(addressRange, that.addressRange) && ipVersion == that.ipVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(addressRange, ipVersion);
    }
}
