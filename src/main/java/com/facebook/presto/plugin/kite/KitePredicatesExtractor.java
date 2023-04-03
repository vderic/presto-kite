/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.kite;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.plugin.kite.util.KiteSqlUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.kite.util.KiteSqlUtils.toSQLCompatibleString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KitePredicatesExtractor
{
    private final List<KiteColumnHandle> clusteringColumns;
    private final ClusteringPushDownResult clusteringPushDownResult;
    private final TupleDomain<ColumnHandle> predicates;

    public KitePredicatesExtractor(List<KiteColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates)
    {
        this.clusteringColumns = ImmutableList.copyOf(clusteringColumns);
        this.predicates = requireNonNull(predicates, "predicates is null");
        this.clusteringPushDownResult = getClusteringKeysSet(clusteringColumns, predicates);
    }

    public String getClusteringKeyPredicates()
    {
        return clusteringPushDownResult.getDomainQuery();
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraints()
    {
        Map<ColumnHandle, Domain> pushedDown = clusteringPushDownResult.getDomains();
        Map<ColumnHandle, Domain> notPushedDown = new HashMap<>(predicates.getDomains().get());

        if (!notPushedDown.isEmpty() && !pushedDown.isEmpty()) {
            notPushedDown.entrySet().removeAll(pushedDown.entrySet());
        }

        return TupleDomain.withColumnDomains(notPushedDown);
    }

    private static ClusteringPushDownResult getClusteringKeysSet(List<KiteColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates)
    {
        ImmutableMap.Builder<ColumnHandle, Domain> domainsBuilder = ImmutableMap.builder();
        ImmutableList.Builder<String> clusteringColumnSql = ImmutableList.builder();
        int currentClusteringColumn = 0;
        for (KiteColumnHandle columnHandle : clusteringColumns) {
            Domain domain = predicates.getDomains().get().get(columnHandle);
            if (domain == null) {
                continue;
            }
            if (domain.isNullAllowed()) {
                continue;
            }
            String predicateString = null;
            predicateString = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        List<Object> singleValues = new ArrayList<>();
                        List<String> rangeConjuncts = new ArrayList<>();
                        String predicate = null;

                        // TODO: check NOT EQUALS.  presto gives (< and >) instead of !=
                        for (Range range : ranges.getOrderedRanges()) {
                            if (range.isAll()) {
                                return null;
                            }
                            if (range.isSingleValue()) {
                                singleValues.add(KiteSqlUtils.sqlValue(toSQLCompatibleString(range.getSingleValue()),
                                        columnHandle.getColumnType()));
                            }
                            else {
                                if (!range.isLowUnbounded()) {
                                    rangeConjuncts.add(format(
                                            "%s %s %s",
                                            KiteSqlUtils.validColumnName(columnHandle.getName()),
                                            range.isLowInclusive() ? ">=" : ">",
                                            KiteSqlUtils.sqlValue(toSQLCompatibleString(range.getLowBoundedValue()), columnHandle.getColumnType())));
                                }
                                if (!range.isHighUnbounded()) {
                                    rangeConjuncts.add(format(
                                            "%s %s %s",
                                            KiteSqlUtils.validColumnName(columnHandle.getName()),
                                            range.isHighInclusive() ? "<=" : "<",
                                            KiteSqlUtils.sqlValue(toSQLCompatibleString(range.getHighBoundedValue()), columnHandle.getColumnType())));
                                }
                            }
                        }

                        if (!singleValues.isEmpty() && !rangeConjuncts.isEmpty()) {
                            return null;
                        }
                        if (!singleValues.isEmpty()) {
                            if (singleValues.size() == 1) {
                                predicate = KiteSqlUtils.validColumnName(columnHandle.getName()) + " = " + singleValues.get(0);
                            }
                            else {
                                predicate = KiteSqlUtils.validColumnName(columnHandle.getName()) + " IN ("
                                        + Joiner.on(",").join(singleValues) + ")";
                            }
                        }
                        else if (!rangeConjuncts.isEmpty()) {
                            predicate = Joiner.on(" AND ").join(rangeConjuncts);
                        }
                        return predicate;
                    }, discreteValues -> {
                        if (discreteValues.isWhiteList()) {
                            ImmutableList.Builder<Object> discreteValuesList = ImmutableList.builder();
                            for (Object discreteValue : discreteValues.getValues()) {
                                discreteValuesList.add(KiteSqlUtils.sqlValue(toSQLCompatibleString(discreteValue),
                                        columnHandle.getColumnType()));
                            }
                            String predicate = KiteSqlUtils.validColumnName(columnHandle.getName()) + " IN ("
                                    + Joiner.on(",").join(discreteValuesList.build()) + ")";
                            return predicate;
                        }
                        return null;
                    }, allOrNone -> null);

            if (predicateString == null) {
                continue;
            }

            clusteringColumnSql.add(predicateString);
            domainsBuilder.put(columnHandle, domain);
            currentClusteringColumn++;
        }
        List<String> clusteringColumnPredicates = clusteringColumnSql.build();

        return new ClusteringPushDownResult(domainsBuilder.build(), Joiner.on(" AND ").join(clusteringColumnPredicates));
    }

    private static class ClusteringPushDownResult
    {
        private final Map<ColumnHandle, Domain> domains;
        private final String domainQuery;

        public ClusteringPushDownResult(Map<ColumnHandle, Domain> domains, String domainQuery)
        {
            this.domains = requireNonNull(ImmutableMap.copyOf(domains));
            this.domainQuery = requireNonNull(domainQuery);
        }

        public Map<ColumnHandle, Domain> getDomains()
        {
            return domains;
        }

        public String getDomainQuery()
        {
            return domainQuery;
        }
    }
}
