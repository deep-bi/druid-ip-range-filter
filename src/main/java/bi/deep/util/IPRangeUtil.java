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

import bi.deep.entity.IPSetContents;
import bi.deep.range.IPBoundedRange;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressSeqRange;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.format.IPAddressRange;
import inet.ipaddr.ipv4.IPv4Address;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.common.config.NullHandling;

public final class IPRangeUtil {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern DASH_REGEX = Pattern.compile("^([0-9A-Fa-f:.]+)[–-]([0-9A-Fa-f:.]+)$");
    private static final Pattern SLASH_REGEX = Pattern.compile("^([0-9A-Fa-f:.]+)/([0-9A-Fa-f:.]+)$");
    private static final Pattern CIDR_REGEX = Pattern.compile("^[0-9A-Fa-f:.]+/\\d+$");
    private static final Pattern IP_REGEX = Pattern.compile("^[0-9A-Fa-f:.]+$");
    private static final int PARALLEL_LIMIT = 200;

    private IPRangeUtil() {
        throw new AssertionError("No bi.deep.util.IPRangeUtil instances for you!");
    }

    @Nullable
    public static IPAddressRange fromString(String token) {
        Matcher dashMatcher = DASH_REGEX.matcher(token);

        if (dashMatcher.matches()) {
            IPAddress low = new IPAddressString(dashMatcher.group(1)).getAddress();
            IPAddress high = new IPAddressString(dashMatcher.group(2)).getAddress();

            if (low != null && high != null && low.getIPVersion() == high.getIPVersion()) {
                return low.spanWithRange(high);
            }

            return null;
        }

        Matcher slashMatcher = SLASH_REGEX.matcher(token);
        Matcher cidrMatcher = CIDR_REGEX.matcher(token);

        if (slashMatcher.matches() && !cidrMatcher.matches()) {
            IPAddress low = new IPAddressString(slashMatcher.group(1)).getAddress();
            IPAddress high = new IPAddressString(slashMatcher.group(2)).getAddress();

            if (low != null && high != null && low.getIPVersion() == high.getIPVersion()) {
                return low.spanWithRange(high);
            }

            return null;
        }

        return new IPAddressString(token).getAddress();
    }

    public static int getSize(IPAddressRange range) {
        if (range == null) return 0;

        return range.getByteCount() == IPv4Address.BYTE_COUNT ? 20 : 44;
    }

    private static Object parseToken(String data, Map<String, Object> cache) {
        return cache.computeIfAbsent(data, token -> {
            Matcher dashMatcher = DASH_REGEX.matcher(token);

            if (dashMatcher.matches()) {
                String lo = dashMatcher.group(1);
                String hi = dashMatcher.group(2);
                IPAddress la = new IPAddressString(lo).getAddress();
                IPAddress ha = new IPAddressString(hi).getAddress();
                if (la != null && ha != null && la.getIPVersion() == ha.getIPVersion()) {
                    return new IPBoundedRange(lo, hi, false, false);
                }

                return null;
            }

            Matcher slashMatcher = SLASH_REGEX.matcher(token);
            Matcher cidrMatcher = CIDR_REGEX.matcher(token);
            boolean isCidrSpecificFormat = cidrMatcher.matches();

            if (slashMatcher.matches() && !isCidrSpecificFormat) {
                String lo = slashMatcher.group(1);
                String hi = slashMatcher.group(2);

                return new IPBoundedRange(lo, hi, false, false);
            }

            if (isCidrSpecificFormat) {
                IPAddressSeqRange seq = new IPAddressString(token).getAddress().toSequentialRange();
                if (seq != null) {
                    return new IPBoundedRange(seq, false, false);
                }
                return null;
            }

            Matcher ipMatcher = IP_REGEX.matcher(token);

            if (ipMatcher.matches()) {
                return new IPAddressString(token).getAddress();
            }

            return null;
        });
    }

    public static IPSetContents extractIPSetContents(String input) {
        if (StringUtils.isBlank(input)) {
            return new IPSetContents(Collections.emptyList(), Collections.emptyList());
        }

        final Map<String, Object> cache = new ConcurrentHashMap<>();
        final String[] tokens = input.split("\\s*,\\s*");
        Stream<String> tokenStream = Arrays.stream(tokens).filter(StringUtils::isNotBlank);

        if (tokens.length > PARALLEL_LIMIT) {
            tokenStream = tokenStream.parallel();
        }

        final List<Object> parsedResults = tokenStream
                .map(token -> parseToken(token, cache))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        final List<IPAddress> ips = new ArrayList<>();
        final List<IPBoundedRange> ranges = new ArrayList<>();

        parsedResults.forEach(result -> {
            if (result instanceof IPAddress) {
                ips.add((IPAddress) result);
            } else if (result instanceof IPBoundedRange) {
                ranges.add((IPBoundedRange) result);
            }
        });

        return new IPSetContents(ips, ranges);
    }

    public static String getMatchingIPs(String input, List<IPAddress> ips) {
        if (StringUtils.isBlank(input) || CollectionUtils.isEmpty(ips)) {
            return NullHandling.sqlCompatible() ? null : StringUtils.EMPTY;
        }

        IPSetContents ranges = extractIPSetContents(input);

        if (ranges.isEmpty()) {
            return NullHandling.sqlCompatible() ? null : StringUtils.EMPTY;
        }

        // Filter matching IPs
        List<String> matchingIps = ips.stream()
                .filter(ip -> ranges.contains(ip, false))
                .map(IPAddress::toString)
                .collect(Collectors.toList());

        if (matchingIps.isEmpty()) {
            return NullHandling.sqlCompatible() ? null : StringUtils.EMPTY;
        }
        if (matchingIps.size() == 1) {
            return matchingIps.get(0);
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(matchingIps);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting to JSON", e);
        }
    }

    public static List<IPAddress> mapStringsToIps(final Set<String> ips) {
        return ips.stream()
                .map(ip -> new IPAddressString(ip).getAddress())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
