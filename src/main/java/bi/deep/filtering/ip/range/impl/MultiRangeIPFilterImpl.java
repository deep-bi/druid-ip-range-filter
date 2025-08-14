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
package bi.deep.filtering.ip.range.impl;

import bi.deep.filtering.common.IPAddressPredicate;
import bi.deep.filtering.common.IPAddressPredicateFactory;
import bi.deep.range.IPRange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import inet.ipaddr.IPAddress;
import inet.ipaddr.format.IPAddressRange;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv6.IPv6Address;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.index.BitmapColumnIndex;

public class MultiRangeIPFilterImpl implements Filter {
    private final String column;
    private final boolean ignoreVersionMismatch;

    private final long[] v4Starts;
    private final long[] v4Ends;
    private final BigInteger[] v6Starts;
    private final BigInteger[] v6Ends;

    public MultiRangeIPFilterImpl(String column, Set<IPRange> ranges, boolean ignoreVersionMismatch) {
        if (column == null) {
            throw InvalidInput.exception("Column cannot be null");
        }
        if (ranges == null || ranges.isEmpty()) {
            throw InvalidInput.exception("ranges cannot be null or empty");
        }

        this.column = column;
        this.ignoreVersionMismatch = ignoreVersionMismatch;

        List<I4> v4 = new ArrayList<>();
        List<I6> v6 = new ArrayList<>();
        for (IPRange r : ranges) {
            IPAddressRange seq = r.getAddressRange();
            IPAddress lo = seq.getLower();
            IPAddress up = seq.getUpper();
            if (lo.isIPv4()) {
                IPv4Address l4 = (IPv4Address) lo;
                IPv4Address u4 = (IPv4Address) up;
                v4.add(new I4(l4.longValue(), u4.longValue()));
            } else {
                IPv6Address l6 = (IPv6Address) lo;
                IPv6Address u6 = (IPv6Address) up;
                v6.add(new I6(l6.getValue(), u6.getValue()));
            }
        }

        List<I4> m4 = mergeV4(v4);
        List<I6> m6 = mergeV6(v6);

        this.v4Starts = new long[m4.size()];
        this.v4Ends = new long[m4.size()];
        for (int i = 0; i < m4.size(); i++) {
            this.v4Starts[i] = m4.get(i).s;
            this.v4Ends[i] = m4.get(i).e;
        }

        this.v6Starts = new BigInteger[m6.size()];
        this.v6Ends = new BigInteger[m6.size()];
        for (int i = 0; i < m6.size(); i++) {
            this.v6Starts[i] = m6.get(i).s;
            this.v6Ends[i] = m6.get(i).e;
        }
    }

    private static List<I4> mergeV4(List<I4> ivals) {
        if (ivals.isEmpty()) {
            return Collections.emptyList();
        }
        ivals.sort(Comparator.comparingLong(a -> a.s));
        List<I4> out = new ArrayList<>();
        I4 cur = ivals.get(0);
        for (int i = 1; i < ivals.size(); i++) {
            I4 nxt = ivals.get(i);
            if (nxt.s <= cur.e + 1L) {
                if (nxt.e > cur.e) {
                    cur.e = nxt.e;
                }
            } else {
                out.add(cur);
                cur = nxt;
            }
        }
        out.add(cur);
        return out;
    }

    private static List<I6> mergeV6(List<I6> ivals) {
        if (ivals.isEmpty()) {
            return Collections.emptyList();
        }
        ivals.sort(Comparator.comparing(a -> a.s));
        List<I6> out = new ArrayList<>();
        I6 cur = ivals.get(0);
        for (int i = 1; i < ivals.size(); i++) {
            I6 nxt = ivals.get(i);
            if (nxt.s.compareTo(cur.e.add(BigInteger.ONE)) <= 0) {
                if (nxt.e.compareTo(cur.e) > 0) {
                    cur.e = nxt.e;
                }
            } else {
                out.add(cur);
                cur = nxt;
            }
        }
        out.add(cur);
        return out;
    }

    private static int upperBound(long[] arr, long key) {
        int lo = 0;
        int hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid] <= key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private static int upperBound(BigInteger[] arr, BigInteger key) {
        int lo = 0;
        int hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid].compareTo(key) <= 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    @Nullable
    @Override
    public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector columnIndexSelector) {
        return null;
    }

    @Override
    public ValueMatcher makeMatcher(ColumnSelectorFactory factory) {
        return factory.makeDimensionSelector(new DefaultDimensionSpec(column, column))
                .makeValueMatcher(new IPAddressPredicateFactory(IPAddressPredicate.of(this::contains)));
    }

    @VisibleForTesting
    public boolean contains(@NotNull final IPAddress ipAddress) {
        if (ipAddress.isIPv4()) {
            if (v4Starts.length == 0) {
                return ignoreVersionMismatch;
            }
            long val = ((IPv4Address) ipAddress).longValue();
            int i = upperBound(v4Starts, val);
            if (i <= 0) {
                return false;
            }
            return val <= v4Ends[i - 1];
        } else if (ipAddress.isIPv6()) {
            if (v6Starts.length == 0) {
                return ignoreVersionMismatch;
            }
            BigInteger val = ipAddress.getValue();
            int i = upperBound(v6Starts, val);
            if (i <= 0) {
                return false;
            }
            return val.compareTo(v6Ends[i - 1]) <= 0;
        } else {
            return ignoreVersionMismatch;
        }
    }

    @Override
    public Set<String> getRequiredColumns() {
        return ImmutableSet.of(column);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MultiRangeIPFilterImpl)) {
            return false;
        }
        MultiRangeIPFilterImpl that = (MultiRangeIPFilterImpl) o;
        return ignoreVersionMismatch == that.ignoreVersionMismatch
                && Objects.equals(column, that.column)
                && Arrays.equals(v4Starts, that.v4Starts)
                && Arrays.equals(v4Ends, that.v4Ends)
                && Arrays.equals(v6Starts, that.v6Starts)
                && Arrays.equals(v6Ends, that.v6Ends);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(column, ignoreVersionMismatch);
        result = 31 * result + Arrays.hashCode(v4Starts);
        result = 31 * result + Arrays.hashCode(v4Ends);
        result = 31 * result + Arrays.hashCode(v6Starts);
        result = 31 * result + Arrays.hashCode(v6Ends);
        return result;
    }

    private static final class I4 {
        long s, e;

        I4(long s, long e) {
            this.s = s;
            this.e = e;
        }
    }

    private static final class I6 {
        BigInteger s, e;

        I6(BigInteger s, BigInteger e) {
            this.s = s;
            this.e = e;
        }
    }
}
