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
package bi.deep.filtering.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bi.deep.entity.IPSetContents;
import bi.deep.util.IPRangeUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.druid.common.config.NullHandling;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class IPRangeUtilTest {

    @BeforeAll
    static void setup() {
        NullHandling.initializeForTests();
    }

    @Test
    void testExtractIPRanges() {
        String input = "192.168.1.1-192.168.1.100,10.0.0.0/24";
        IPSetContents contents = IPRangeUtil.extractIPSetContents(input);
        assertEquals(2, contents.getRanges().size());
    }

    @Test
    void testGetMatchingIPs_EmptyResult() {
        String input = "192.168.1.1-192.168.1.100";
        List<IPAddress> ips = IPRangeUtil.mapStringsToIps(Sets.newHashSet("10.0.0.1"));
        assertEquals(NullHandling.sqlCompatible() ? null : "", IPRangeUtil.getMatchingIPs(input, ips));
    }

    @Test
    void testGetMatchingIPs_SingleMatch() {
        String input = "192.168.1.1-192.168.1.100";
        List<IPAddress> ips = IPRangeUtil.mapStringsToIps(Sets.newHashSet("192.168.1.50"));
        assertEquals("192.168.1.50", IPRangeUtil.getMatchingIPs(input, ips));
    }

    @Test
    void testGetMatchingIPs_MultipleMatches() throws JsonProcessingException {
        String input = "192.168.1.1-192.168.1.100,10.0.0.0/24";
        List<IPAddress> ips = IPRangeUtil.mapStringsToIps(Sets.newHashSet("192.168.1.50", "10.0.0.1"));
        String expectedJson = new ObjectMapper().writeValueAsString(Arrays.asList("192.168.1.50", "10.0.0.1"));
        assertEquals(expectedJson, IPRangeUtil.getMatchingIPs(input, ips));
    }

    @Test
    void testMapStringsToIps() {
        Set<String> ips = Sets.newHashSet("192.168.1.1", "10.0.0.1");
        List<IPAddress> result = IPRangeUtil.mapStringsToIps(ips);
        assertEquals(2, result.size());
        assertEquals(new IPAddressString("192.168.1.1").getAddress(), result.get(0));
    }

    @Test
    void testContainsAnyIP() {
        String input = "192.168.1.1-192.168.1.100";
        IPSetContents ranges = IPRangeUtil.extractIPSetContents(input);
        List<IPAddress> ips = IPRangeUtil.mapStringsToIps(Sets.newHashSet("192.168.1.50"));
        assertTrue(ranges.containsAnyIP(ips, false));
    }

    @Test
    void testContainsAnyIP_NoMatch() {
        String input = "192.168.1.1-192.168.1.100";
        IPSetContents ranges = IPRangeUtil.extractIPSetContents(input);
        List<IPAddress> ips = IPRangeUtil.mapStringsToIps(Sets.newHashSet("10.0.0.1"));
        assertFalse(ranges.containsAnyIP(ips, false));
    }

    @Test
    void testContainsSingleIp_Match() {
        String input = "192.168.1.1";
        IPSetContents ranges = IPRangeUtil.extractIPSetContents(input);
        List<IPAddress> ips = IPRangeUtil.mapStringsToIps(Sets.newHashSet("192.168.1.1"));
        assertTrue(ranges.containsAnyIP(ips, false));
    }

    @Test
    void testContainsSingleIp_NoMatch() {
        String input = "192.168.1.1";
        IPSetContents ranges = IPRangeUtil.extractIPSetContents(input);
        List<IPAddress> ips = IPRangeUtil.mapStringsToIps(Sets.newHashSet("192.168.1.2"));
        assertFalse(ranges.containsAnyIP(ips, false));
    }

    @Test
    void testContainsMixedWithRange_Match() {
        String input = "192.168.1.1, 192.168.1.3-192.168.1.100";
        IPSetContents ranges = IPRangeUtil.extractIPSetContents(input);
        List<IPAddress> ips = IPRangeUtil.mapStringsToIps(Sets.newHashSet("192.168.1.1"));
        assertTrue(ranges.containsAnyIP(ips, false));
    }

    @Test
    void testContainsMixedWithRange_NoMatch() {
        String input = "192.168.1.1, 192.168.1.3-192.168.1.100";
        IPSetContents ranges = IPRangeUtil.extractIPSetContents(input);
        List<IPAddress> ips = IPRangeUtil.mapStringsToIps(Sets.newHashSet("192.168.1.2"));
        assertFalse(ranges.containsAnyIP(ips, false));
    }
}
