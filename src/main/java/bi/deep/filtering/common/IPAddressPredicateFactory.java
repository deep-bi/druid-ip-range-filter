package bi.deep.filtering.common;

import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;

public class IPAddressPredicateFactory implements DruidPredicateFactory {
    private final IPAddressPredicate predicate;

    public IPAddressPredicateFactory(IPAddressPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public DruidObjectPredicate<String> makeStringPredicate() {
        return predicate;
    }

    @Override
    public DruidLongPredicate makeLongPredicate() {
        return null; // No Op
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate() {
        return null; // No op
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate() {
        return null ; // No Op
    }
}
