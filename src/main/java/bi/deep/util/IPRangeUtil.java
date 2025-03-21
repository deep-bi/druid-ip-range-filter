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
package bi.deep.util;

import bi.deep.range.IPBoundedRange;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressSeqRange;
import inet.ipaddr.IPAddressString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.common.config.NullHandling;

public class IPRangeUtil {
    private static final Pattern RANGE_PATTERN = Pattern.compile("([0-9a-fA-F:.]+)-([0-9a-fA-F:.]+)");
    private static final Pattern CIDR_PATTERN = Pattern.compile("([0-9a-fA-F:.]+)/([0-9]+)");

    public static IPBoundedRange[] extractIPRanges(String input) {
        List<IPBoundedRange> ranges = new ArrayList<>();
        Matcher matcher = RANGE_PATTERN.matcher(input);
        while (matcher.find()) {
            ranges.add(new IPBoundedRange(matcher.group(1), matcher.group(2), false, false));
        }

        Matcher cidrMatcher = CIDR_PATTERN.matcher(input);
        while (cidrMatcher.find()) {
            String cidrStr = cidrMatcher.group();
            IPAddressSeqRange ipAddressSeqRange =
                    new IPAddressString(cidrStr).getAddress().toSequentialRange();
            if (ipAddressSeqRange != null) {
                ranges.add(new IPBoundedRange(ipAddressSeqRange, false, false));
            }
        }
        return ranges.toArray(new IPBoundedRange[0]);
    }

    public static String getMatchingIPs(String input, Set<String> ips) {
        List<String> matchingIps = mapStringsToIps(ips).stream()
                .filter(ip -> Arrays.stream(extractIPRanges(input))
                        .anyMatch(range -> range.contains(ip, false)))
                .map(IPAddress::toString)
                .collect(Collectors.toList());
        if (matchingIps.isEmpty()) {
            return NullHandling.sqlCompatible() ? null : StringUtils.EMPTY;
        } else if (matchingIps.size() == 1) {
            return matchingIps.get(0);
        } else {
            try {
                return new ObjectMapper().writeValueAsString(matchingIps);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Error converting to JSON", e);
            }
        }
    }

    public static List<IPAddress> mapStringsToIps(final Set<String> ips) {
        return ips.stream()
                .map(ip -> new IPAddressString(ip).getAddress())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public static boolean containsAnyIP(IPBoundedRange[] ranges, List<IPAddress> ips, boolean ignoreVersionMismatch) {
        return Arrays.stream(ranges).anyMatch(range -> range.containsAnyIP(ips, ignoreVersionMismatch));
    }
}
