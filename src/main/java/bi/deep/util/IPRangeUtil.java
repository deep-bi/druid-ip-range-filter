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
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.common.config.NullHandling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class IPRangeUtil
{
  private static final String DASH_REGEX = "^([0-9A-Fa-f:.]+)[–-]([0-9A-Fa-f:.]+)$";
  private static final String SLASH_REGEX = "^([0-9A-Fa-f:.]+)/([0-9A-Fa-f:.]+)$";
  private static final String CIDR_REGEX = "^[0-9A-Fa-f:.]+/\\d+$";
  private static final String IP_REGEX = "^[0-9A-Fa-f:.]+$";

  public static IPSetContents extractIPSetContents(String input)
  {
    List<IPAddress> ips = new ArrayList<>();
    List<IPBoundedRange> ranges = new ArrayList<>();

    for (String token : input.split("\\s*,\\s*")) {
        if (token.isEmpty()) {
            continue;
        }

      if (token.matches(DASH_REGEX)) {
        Matcher m = Pattern.compile(DASH_REGEX).matcher(token);
        if (m.matches()) {
          String lo = m.group(1);
          String hi = m.group(2);
          IPAddress la = new IPAddressString(lo).getAddress();
          IPAddress ha = new IPAddressString(hi).getAddress();
          if (la != null && ha != null && la.getIPVersion() == ha.getIPVersion()) {
            ranges.add(new IPBoundedRange(lo, hi, false, false));
          }
        }
        continue;
      }

      if (token.matches(SLASH_REGEX) && !token.matches(CIDR_REGEX)) {
        Matcher m = Pattern.compile(SLASH_REGEX).matcher(token);
        if (m.matches()) {
          String lo = m.group(1);
          String hi = m.group(2);
          ranges.add(new IPBoundedRange(lo, hi, false, false));
        }
        continue;
      }

      if (token.matches(CIDR_REGEX)) {
        IPAddressSeqRange seq = new IPAddressString(token).getAddress().toSequentialRange();
        if (seq != null) {
          ranges.add(new IPBoundedRange(seq, false, false));
        }
        continue;
      }

      if (token.matches(IP_REGEX)) {
        IPAddress addr = new IPAddressString(token).getAddress();
        if (addr != null) {
          ips.add(addr);
        }
      }
    }

    return new IPSetContents(ips, ranges);
  }

  public static String getMatchingIPs(String input, Set<String> ips)
  {
    List<String> matchingIps = mapStringsToIps(ips).stream()
                                                   .filter(ip ->
                                                               Optional.ofNullable(extractIPSetContents(input).getRanges())
                                                                       .orElse(Collections.emptyList())
                                                                       .stream()
                                                                       .anyMatch(r -> r.contains(ip, false))
                                                   )
                                                   .map(IPAddress::toString)
                                                   .collect(Collectors.toList());
    if (matchingIps.isEmpty()) {
      return NullHandling.sqlCompatible() ? null : StringUtils.EMPTY;
    } else if (matchingIps.size() == 1) {
      return matchingIps.get(0);
    } else {
      try {
        return new ObjectMapper().writeValueAsString(matchingIps);
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException("Error converting to JSON", e);
      }
    }
  }

  public static List<IPAddress> mapStringsToIps(final Set<String> ips)
  {
    return ips.stream()
              .map(ip -> new IPAddressString(ip).getAddress())
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
  }
}
