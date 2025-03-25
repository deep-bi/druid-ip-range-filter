package bi.deep.filtering.fn;

import bi.deep.matching.IPRangeFilteredExtractionFn;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.common.config.NullHandling;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IPRangeFilteredExtractionFnTest
{

  private IPRangeFilteredExtractionFn extractionFn;

  static Stream<Arguments> provideTestCases()
  {
    return Stream.of(
        Arguments.of(NullHandlingMode.NON_SQL_COMPATIBLE, null, ""),
        Arguments.of(NullHandlingMode.SQL_COMPATIBLE, null, null),
        Arguments.of(NullHandlingMode.NON_SQL_COMPATIBLE, "10.162.59.18-10.162.59.20", "10.162.59.19"),
        Arguments.of(NullHandlingMode.SQL_COMPATIBLE, "10.161.12.13-10.161.12.15", "10.161.12.13"),
        Arguments.of(NullHandlingMode.NON_SQL_COMPATIBLE, "10.162.59.18-10.162.59.20, weird string", "10.162.59.19"),
        Arguments.of(NullHandlingMode.SQL_COMPATIBLE, "10.161.12.13-10.161.12.15, weird string", "10.161.12.13"),
        Arguments.of(NullHandlingMode.NON_SQL_COMPATIBLE, "weird string", StringUtils.EMPTY),
        Arguments.of(NullHandlingMode.SQL_COMPATIBLE, "weird string", null),
        Arguments.of(NullHandlingMode.NON_SQL_COMPATIBLE, "192.168.1.1", StringUtils.EMPTY),
        Arguments.of(NullHandlingMode.SQL_COMPATIBLE, "172.16.0.1", null)
    );
  }

  @BeforeEach
  void setUp()
  {
    Set<String> testIps = new HashSet<>(Arrays.asList("10.162.59.19", "10.161.12.13"));
    extractionFn = new IPRangeFilteredExtractionFn(testIps);
  }

  private void configureNullHandling(NullHandlingMode mode)
  {
    if (mode == NullHandlingMode.SQL_COMPATIBLE) {
      NullHandling.initializeForTestsWithValues(false, false, false);
    } else {
      NullHandling.initializeForTestsWithValues(true, true, true);
    }
  }

  @ParameterizedTest
  @MethodSource("provideTestCases")
  void testApply(NullHandlingMode mode, String input, String expected)
  {
    configureNullHandling(mode);
    assertEquals(expected, extractionFn.apply(input), "Unexpected extraction result");
  }

  enum NullHandlingMode
  {
    SQL_COMPATIBLE, NON_SQL_COMPATIBLE
  }
}