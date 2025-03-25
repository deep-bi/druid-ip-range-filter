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
package bi.deep.filtering.common;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;

public class IPAddressRangeListPredicateFactory implements DruidPredicateFactory {
    private final IPAddressRangeListPredicate predicate;

    public IPAddressRangeListPredicateFactory(IPAddressRangeListPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public DruidObjectPredicate<String> makeStringPredicate() {
        return predicate;
    }

    @Override
    public DruidLongPredicate makeLongPredicate() {
        return null;
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate() {
        return null;
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate() {
        return null;
    }

    @Override
    public DruidObjectPredicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> inputType) {
        return o -> predicate.apply(Objects.toString(inputType, null));
    }

    @Override
    public DruidObjectPredicate<Object> makeObjectPredicate() {
        return null;
    }
}
