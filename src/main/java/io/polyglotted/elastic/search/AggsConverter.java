package io.polyglotted.elastic.search;

import io.polyglotted.elastic.search.Aggregation.AggregationType;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.join.aggregations.Children;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.elastic.search.Aggregation.aggregationBuilder;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum AggsConverter {
    AggMax {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            return aggregationBuilder().label(label).type(AggregationType.Max).value("Max", ((Max) agg).getValue());
        }
    },
    AggMin {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            return aggregationBuilder().label(label).type(AggregationType.Min).value("Min", ((Min) agg).getValue());
        }
    },
    AggSum {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            return aggregationBuilder().label(label).type(AggregationType.Sum).value("Sum", ((Sum) agg).getValue());
        }
    },
    AggAvg {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            return aggregationBuilder().label(label).type(AggregationType.Avg).value("Avg", ((Avg) agg).getValue());
        }
    },
    AggCount {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            return aggregationBuilder().label(label).type(AggregationType.Count).value("Count", ((ValueCount) agg).getValue());
        }
    },
    AggCardinality {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            return aggregationBuilder().label(label).type(AggregationType.Cardinality).value("Cardinality", ((Cardinality) agg).getValue());
        }
    },
    AggExtStatistics {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            ExtendedStats stats = (ExtendedStats) agg;
            return aggregationBuilder().label(label).type(AggregationType.ExtStatistics).value("Count", stats.getCount())
                .value("Max", stats.getMax()).value("Min", stats.getMin()).value("Avg", stats.getAvg()).value("Sum", stats.getSum())
                .value("SumOfSquares", stats.getSumOfSquares()).value("StdDeviation", stats.getStdDeviation()).value("Variance", stats.getVariance());
        }
    },
    AggStatistics {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            Stats stats = (Stats) agg;
            return aggregationBuilder().label(label).type(AggregationType.Statistics).value("Count", stats.getCount()).value("Max",
                stats.getMax()).value("Min", stats.getMin()).value("Avg", stats.getAvg()).value("Sum", stats.getSum());
        }
    },
    AggTerm {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            Terms terms = (Terms) agg;
            Aggregation.Builder builder = aggregationBuilder().label(label).type(AggregationType.Term)
                .param("docCountError", terms.getDocCountError()).param("sumOfOtherDocs", terms.getSumOfOtherDocCounts());

            for (Terms.Bucket bucket : terms.getBuckets()) {
                Bucket.Builder buckBldr = builder.bucketBuilder().key(bucket.getKeyAsString())
                    .value(bucket.getKey()).count(bucket.getDocCount());

                if (bucket.getAggregations() == null) continue;
                for (org.elasticsearch.search.aggregations.Aggregation child : bucket.getAggregations()) { buckBldr.aggregation(detectAgg(child)); }
            }
            return builder;
        }
    },
    AggDateHistogram {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            Histogram histogram = (Histogram) agg;
            Aggregation.Builder builder = aggregationBuilder().label(label).type(AggregationType.DateHistogram);

            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                Bucket.Builder buckBldr = builder.bucketBuilder().key(bucket.getKeyAsString())
                    .value(bucket.getKey()).count(bucket.getDocCount());

                if (bucket.getAggregations() == null) continue;
                for (org.elasticsearch.search.aggregations.Aggregation child : bucket.getAggregations()) { buckBldr.aggregation(detectAgg(child)); }
            }
            return builder;
        }
    },
    AggFilter {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            Aggregation.Builder builder = aggregationBuilder().label(label).type(AggregationType.Filter);
            return addSingleBucketChildren(label, builder, (Filter) agg);
        }
    },
    AggChildren {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            Aggregation.Builder builder = aggregationBuilder().label(label).type(AggregationType.Children);
            return addSingleBucketChildren(label, builder, (Children) agg);
        }
    },
    AggNested {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            Aggregation.Builder builder = aggregationBuilder().label(label).type(AggregationType.Nested);
            return addSingleBucketChildren(label, builder, (Nested) agg);
        }
    },
    AggReverseNested {
        @Override Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg) {
            Aggregation.Builder builder = aggregationBuilder().label(label).type(AggregationType.ReverseNested);
            return addSingleBucketChildren(label, builder, (ReverseNested) agg);
        }
    };

    abstract Aggregation.Builder getWith(String label, org.elasticsearch.search.aggregations.Aggregation agg);

    static Aggregation.Builder detectAgg(org.elasticsearch.search.aggregations.Aggregation agg) {
        AggsConverter converter = null;
        if (agg instanceof Max) { converter = AggMax; }
        else if (agg instanceof Min) { converter = AggMin; }
        else if (agg instanceof Sum) { converter = AggSum; }
        else if (agg instanceof Avg) { converter = AggAvg; }
        else if (agg instanceof ValueCount) { converter = AggCount; }
        else if (agg instanceof Cardinality) { converter = AggCardinality; }
        else if (agg instanceof ExtendedStats) { converter = AggExtStatistics; }
        else if (agg instanceof Stats) { converter = AggStatistics; }
        else if (agg instanceof Terms) { converter = AggTerm; }
        else if (agg instanceof Histogram) { converter = AggDateHistogram; }
        else if (agg instanceof Filter) { converter = AggFilter; }
        else if (agg instanceof Children) { converter = AggChildren; }
        else if (agg instanceof Nested) { converter = AggNested; }
        else if (agg instanceof ReverseNested) { converter = AggReverseNested; }
        return checkNotNull(converter, "unable to detect " + agg.getName() + ":" + agg.getClass()).getWith(agg.getName(), agg);
    }

    private static Aggregation.Builder addSingleBucketChildren(String label, Aggregation.Builder builder, SingleBucketAggregation single) {
        Bucket.Builder bucket = builder.bucketBuilder().key(label).count(single.getDocCount());
        for (org.elasticsearch.search.aggregations.Aggregation child : single.getAggregations()) { bucket.aggregation(detectAgg(child));}
        return builder;
    }
}