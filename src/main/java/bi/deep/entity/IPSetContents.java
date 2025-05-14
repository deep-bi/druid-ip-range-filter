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
  public List<IPAddress> getIpAddresses()
  {
    return ipAddresses;
  }

  @Nullable
  public List<IPBoundedRange> getRanges()
  {
    return ranges;
  }
}
