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
package bi.deep.guice;

import bi.deep.filtering.ip.range.FixedSetIPFilter;
import bi.deep.filtering.ip.range.MultiRangeIPFilter;
import bi.deep.filtering.ip.range.RangeMatchingIPFilter;
import bi.deep.filtering.ip.range.SingleTypeIPRangeFilter;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import java.util.Collections;
import java.util.List;
import org.apache.druid.initialization.DruidModule;

public class IPRangeFilterModule implements DruidModule {

    @Override
    public List<? extends Module> getJacksonModules() {
        return Collections.singletonList(new SimpleModule(getClass().getSimpleName())
                .registerSubtypes(FixedSetIPFilter.class)
                .registerSubtypes(MultiRangeIPFilter.class)
                .registerSubtypes(SingleTypeIPRangeFilter.class)
                .registerSubtypes(RangeMatchingIPFilter.class));
    }

    @Override
    public void configure(Binder binder) {
        // NoOp
    }
}
