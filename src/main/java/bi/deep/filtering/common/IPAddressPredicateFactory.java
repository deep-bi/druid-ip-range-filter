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
        return null; // No Op
    }
}
