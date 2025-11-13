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

import bi.deep.entity.dimension.IPRangeArray;
import java.util.List;
import java.util.Objects;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;

public class IPNativeStringifyExprMacro implements ExprMacroTable.ExprMacro {
    private static final String FN_NAME = "ip_native_stringify";

    @Override
    public Expr apply(List<Expr> args) {
        class IPNativeExtractExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr {
            public IPNativeExtractExpr(List<Expr> macroArgs) {
                super(IPNativeStringifyExprMacro.this, macroArgs);
            }

            @Override
            public ExprEval<?> eval(ObjectBinding bindings) {
                ExprEval<?> input = args.get(0).eval(bindings);

                if (input.value() == null) {
                    return ExprEval.ofStringArray(null);
                }

                if (input.value() instanceof IPRangeArray) {
                    return ExprEval.ofStringArray(((IPRangeArray) input.value())
                            .getAddressRanges().stream().map(Objects::toString).toArray());
                }

                return ExprEval.ofStringArray(new String[] {Objects.toString(input.value())});
            }
        }

        return new IPNativeExtractExpr(args);
    }

    @Override
    public String name() {
        return FN_NAME;
    }
}
