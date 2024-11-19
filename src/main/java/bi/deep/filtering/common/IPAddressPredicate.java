package bi.deep.filtering.common;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateMatch;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Predicate;

public class IPAddressPredicate implements DruidObjectPredicate<String>
{
    private final Predicate<IPAddress> predicate;

    public IPAddressPredicate(Predicate<IPAddress> predicate) {
        this.predicate = predicate;
    }

    public static IPAddressPredicate of(Predicate<IPAddress> predicate) {
        return new IPAddressPredicate(predicate);
    }

    @Override
    public DruidPredicateMatch apply(@Nullable String value)
    {
        return mapToIPAddress(value)
            .map(predicate::test)
            .map(DruidPredicateMatch::of)
            .orElse(DruidPredicateMatch.UNKNOWN);
    }

    private Optional<IPAddress> mapToIPAddress(@Nullable String value) {
        return Optional.ofNullable(new IPAddressString(value).getAddress());
    }
}
