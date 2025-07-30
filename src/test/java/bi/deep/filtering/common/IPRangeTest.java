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
package bi.deep.filtering.common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bi.deep.range.IPRange;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import java.util.stream.LongStream;
import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IPRangeTest {

    @Test
    void testIP4Creation() {
        final IPRange ipV4Range = new IPRange("0.0.0.0/255.255.255.255");

        assertNotNull(ipV4Range);
        assertEquals("0.0.0.0", ipV4Range.getLower().toString());
        assertEquals("255.255.255.255", ipV4Range.getUpper().toString());
        assertTrue(ipV4Range.isIPv4());
        assertFalse(ipV4Range.isIPv6());
    }

    @Test
    void testIP6Creation() {
        final IPRange ipV6Range =
                new IPRange("6f:ad2f:938:5f8f:7f94:ddd0:e1a5:4f/58db:b320:7914:41f9:12ca:5ccc:9c05:cd1e");

        assertNotNull(ipV6Range);
        assertEquals("6f:ad2f:938:5f8f:7f94:ddd0:e1a5:4f", ipV6Range.getLower().toString());
        assertEquals(
                "58db:b320:7914:41f9:12ca:5ccc:9c05:cd1e", ipV6Range.getUpper().toString());
        assertTrue(ipV6Range.isIPv6());
        assertFalse(ipV6Range.isIPv4());
    }

    @ParameterizedTest
    @ValueSource(strings = {"39.181.2.192", "6f:ad2f:938:5f8f:7f94:ddd0:e1a5:4f"})
    void rangeContainsTest(String addressStr) {
        final long count = 10;
        final IPAddress refAddress = new IPAddressString(addressStr).getAddress();
        final IPAddress lower = refAddress.increment(count);
        final IPAddress upper = lower.increment(count);
        final IPRange ipV4Range = new IPRange(String.format("%s/%s", lower, upper));

        assertNotNull(ipV4Range);
        assertTrue(ipV4Range.contains(lower));
        assertTrue(ipV4Range.contains(upper));

        // Within Range
        assertTrue(LongStream.range(0, count).mapToObj(lower::increment).allMatch(ipV4Range::contains));
        // Lower than Range
        assertTrue(LongStream.range(0, count).mapToObj(refAddress::increment).noneMatch(ipV4Range::contains));
        // More than Range
        assertTrue(LongStream.range(1, count).mapToObj(upper::increment).noneMatch(ipV4Range::contains));
    }

    @Test
    void testInvalidRangeWithNull() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange(null));
        assertEquals("Range cannot be null or empty", exception.getMessage());
    }

    @Test
    void testInvalidRangeWithEmpty() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange(""));
        assertEquals("Range cannot be null or empty", exception.getMessage());
    }

    @Test
    void testInvalidRangeWithInvalidString() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange("invalidLowerIP/0.0.0.0"));
        assertEquals("Invalid lower IP 'invalidLowerIP/0.0.0.0'.", exception.getMessage());
    }

    @Test
    void testInvalidRangeWithInvalidUpperString() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange("0.0.0.0/invalidUpperIP"));
        assertEquals("Invalid upper IP '0.0.0.0/invalidUpperIP'.", exception.getMessage());
    }

    @Test
    void testInvalidRangeWithInvalidSeparator() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange("0.0.0.0-127.0.1.0"));
        assertEquals(
                "Malformed input '0.0.0.0-127.0.1.0'. Expected IP address, ip/prefix (CIDR) or lower/upper.",
                exception.getMessage());
    }

    @Test
    void testValidSingleIpv4() {
        assertDoesNotThrow(() -> new IPRange("192.0.2.5"));
    }

    @Test
    void testValidSingleIpv6() {
        assertDoesNotThrow(() -> new IPRange("2001:db8::1"));
    }

    @Test
    void testValidCidrIpv4() {
        assertDoesNotThrow(() -> new IPRange("10.0.0.0/24"));
    }

    @Test
    void testValidCidrIpv6() {
        assertDoesNotThrow(() -> new IPRange("2001:db8::/48"));
    }

    @Test
    void testValidRangeIpv4() {
        assertDoesNotThrow(() -> new IPRange("10.0.0.1/10.0.0.20"));
    }

    @Test
    void testValidCidrIpv4Prefix0() {
        assertDoesNotThrow(() -> new IPRange("0.0.0.0/0"));
    }

    @Test
    void testValidCidrIpv4Prefix32() {
        assertDoesNotThrow(() -> new IPRange("255.255.255.255/32"));
    }

    @Test
    void testValidCidrIpv6Prefix0() {
        assertDoesNotThrow(() -> new IPRange("::/0"));
    }

    @Test
    void testValidCidrIpv6Prefix128() {
        assertDoesNotThrow(() -> new IPRange("2001:db8::1/128"));
    }

    @Test
    void testValidTrimmedSingleIp() {
        assertDoesNotThrow(() -> new IPRange("  10.1.1.1  "));
    }

    @Test
    void testValidIpv6LowerUpperRange() {
        assertDoesNotThrow(() -> new IPRange("2001:db8::1/2001:db8::ff"));
    }

    @Test
    void testValidSameIpRangeIpv4() {
        assertDoesNotThrow(() -> new IPRange("10.0.0.1/10.0.0.1"));
    }

    @Test
    void testInvalidRangeWithOnlyLowerAndSeparator() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange("10.0.0.1/"));
        assertEquals("Malformed range '10.0.0.1/'. Expected ip/prefix (CIDR) or lower/upper.", exception.getMessage());
    }

    @Test
    void testInvalidRangeWithOnlyPrefix() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange("/24"));
        assertEquals("Malformed range '/24'. Expected ip/prefix (CIDR) or lower/upper.", exception.getMessage());
    }

    @Test
    void testInvalidMixedVersions() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new IPRange("10.0.0.1/::1"));
        assertEquals("IPv4/IPv6 mismatch: '10.0.0.1' vs '::1'.", exception.getMessage());
    }

    @Test
    void testInvalidSingleIp() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange("999.999.999.999"));
        assertEquals(
                "Malformed input '999.999.999.999'. Expected IP address, ip/prefix (CIDR) or lower/upper.",
                exception.getMessage());
    }

    @Test
    void testInvalidCidrIpv6Prefix129() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange("2001:db8::/129"));
        assertEquals("Malformed CIDR '2001:db8::/129'. Expected ip/prefix.", exception.getMessage());
    }

    @Test
    void testInvalidCidrPrefixTooLarge() {
        DruidException exception = assertThrows(DruidException.class, () -> new IPRange("10.0.0.0/33"));
        assertEquals("Malformed CIDR '10.0.0.0/33'. Expected ip/prefix.", exception.getMessage());
    }

    @Test
    void singleIpContainsItself() {
        IPRange r = new IPRange("192.0.2.5");
        IPAddress addr = new IPAddressString("192.0.2.5").getAddress();
        assertTrue(r.contains(addr));
    }

    @Test
    void singleIpDoesNotContainDifferent() {
        IPRange r = new IPRange("192.0.2.5");
        IPAddress addr = new IPAddressString("192.0.2.6").getAddress();
        assertFalse(r.contains(addr));
    }

    @Test
    void cidrContainsAddressInside() {
        IPRange r = new IPRange("10.0.0.0/24");
        IPAddress addr = new IPAddressString("10.0.0.128").getAddress();
        assertTrue(r.contains(addr));
    }

    @Test
    void cidrDoesNotContainAddressOutside() {
        IPRange r = new IPRange("10.0.0.0/24");
        IPAddress addr = new IPAddressString("10.0.1.1").getAddress();
        assertFalse(r.contains(addr));
    }

    @Test
    void rangeContainsLowerBoundary() {
        IPRange r = new IPRange("10.0.0.5/10.0.0.10");
        IPAddress addr = new IPAddressString("10.0.0.5").getAddress();
        assertTrue(r.contains(addr));
    }

    @Test
    void rangeContainsUpperBoundary() {
        IPRange r = new IPRange("10.0.0.5/10.0.0.10");
        IPAddress addr = new IPAddressString("10.0.0.10").getAddress();
        assertTrue(r.contains(addr));
    }

    @Test
    void rangeContainsMiddleAddress() {
        IPRange r = new IPRange("10.0.0.5/10.0.0.10");
        IPAddress addr = new IPAddressString("10.0.0.8").getAddress();
        assertTrue(r.contains(addr));
    }

    @Test
    void rangeDoesNotContainBelowLower() {
        IPRange r = new IPRange("10.0.0.5/10.0.0.10");
        IPAddress addr = new IPAddressString("10.0.0.4").getAddress();
        assertFalse(r.contains(addr));
    }

    @Test
    void rangeDoesNotContainAboveUpper() {
        IPRange r = new IPRange("10.0.0.5/10.0.0.10");
        IPAddress addr = new IPAddressString("10.0.0.11").getAddress();
        assertFalse(r.contains(addr));
    }

    @Test
    void reversedRangeStillContains() {
        IPRange r = new IPRange("10.0.0.10/10.0.0.5");
        IPAddress addr = new IPAddressString("10.0.0.7").getAddress();
        assertTrue(r.contains(addr));
    }

    @Test
    void ipv6RangeContains() {
        IPRange r = new IPRange("2001:db8::1/2001:db8::ff");
        IPAddress addr = new IPAddressString("2001:db8::80").getAddress();
        assertTrue(r.contains(addr));
    }

    @Test
    void ipv6RangeDoesNotContain() {
        IPRange r = new IPRange("2001:db8::1/2001:db8::ff");
        IPAddress addr = new IPAddressString("2001:db8:1::1").getAddress();
        assertFalse(r.contains(addr));
    }
}
