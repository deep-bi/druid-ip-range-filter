package bi.deep.entity;

import bi.deep.range.IPBoundedRange;
import inet.ipaddr.IPAddress;

import javax.annotation.Nullable;
import java.util.List;


public class IPSetContents
{

  @Nullable
  private final List<IPAddress> ipAddresses;
  @Nullable

  private final List<IPBoundedRange> ranges;

  public IPSetContents(@Nullable List<IPAddress> ipAddresses, @Nullable List<IPBoundedRange> ranges)
  {
    this.ipAddresses = ipAddresses;
    this.ranges = ranges;
  }

  @Nullable
  public List<IPBoundedRange> getRanges()
  {
    return ranges;
  }

  public boolean containsAnyIP(List<IPAddress> candidates, boolean ignoreVersionMismatch)
  {
    for (IPAddress ip : candidates) {
      if (ipAddresses != null && !ipAddresses.isEmpty() && ipAddresses
              .contains(ip)) {
        return true;
      }
    }
    return ranges != null && ranges.stream()
            .anyMatch(r -> r.containsAnyIP(candidates, ignoreVersionMismatch));
  }
}
