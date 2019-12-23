import bisect
from collections.abc import Iterable
from operator import itemgetter
import warnings

import dawg
from toolz.curried import filter, pipe, map
from toolz.itertoolz import partition_all, peek
from toolz.functoolz import identity, compose

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

__all__ = ["DawgLookup"]


def _dawg_with_bounds(xs, dawg_impl=dawg.IntDAWG):
    """
    :param xs: Iterable[Tuple[str, T]] where T is a type acceptable as a key for dawg_impl
    :param dawg_impl: Type[dawg.DAWG]
    """

    try:
        x, xs = peek(xs)
        has_index = False if isinstance(x, str) else True
        min_, max_ = (x[0], x[0]) if has_index else (x, x)
    except StopIteration:
        has_index = False
        min_, max_ = None, None
        xs = []

    def _with_stats(xs):
        nonlocal min_
        nonlocal max_
        for x in xs:
            token = x[0] if has_index else x

            if x is not None:
                min_ = token if token < min_ else min_
                max_ = token if token > max_ else max_
                yield x

    dawg_ = dawg_impl(_with_stats(xs))
    return min_, max_, dawg_


class DawgLookup:
    @staticmethod
    def chunked_partition_mapper(max_records_per_trie):
        def _(xs):
            try:
                _, xs = peek(xs)
                for chunk in partition_all(max_records_per_trie, xs):
                    yield _dawg_with_bounds(chunk)
            except StopIteration:
                pass

        return _

    @staticmethod
    def simple_partition_mapper(xs):
        try:
            _, xs = peek(xs)
            yield _dawg_with_bounds(xs)
        except StopIteration:
            pass

    @staticmethod
    def _prepare_local(xs, has_index, sort):
        """
        Prepares local data for building lookup list
        """

        return pipe(
            xs,
            filter(itemgetter(0) if has_index else identity),
            sorted if sort else identity,
        )

    @staticmethod
    def _prepare_spark(df, num_partitions, has_index, sort):
        """
        Prepares Spark DataFrame for building lookup list
        """
        num_partitions = num_partitions or df.rdd.getNumPartitions()
        cols = (
            ["token", "tokenid"]
            if has_index and "tokenid" in df.columns
            # Add dummy index so downstream code
            else ["token"]
        )

        row_mapper = identity if len(cols) == 2 else itemgetter(0)

        if sort:
            df = df.repartitionByRange(num_partitions, "token").sortWithinPartitions(
                "token"
            )
        return (
            df.select(*cols)
            .na.drop()  # This shouldn't happen but let's be sure
            .filter(F.length(F.trim(F.col("token"))) != 0)
            .rdd.map(row_mapper)
        )

    @staticmethod
    def _lookup_from_spark(df, num_partitions, has_index, sort, partition_mapper):
        """
        Build lookup / bounds list from Spark DataFrame
        """
        lookups = sorted(
            DawgLookup._prepare_spark(df, num_partitions, has_index, sort)
            .mapPartitions(partition_mapper)
            .collect(),
            key=itemgetter(0, 1),
        )
        return lookups

    @staticmethod
    def _lookup_from_local(xs, has_index, sort, partition_mapper):
        """
        Build lookup / bounds list from local collection
        """
        try:
            x, xs = peek(xs)
            assert has_index or isinstance(x, str)
        except StopIteration:
            xs = []

        return pipe(
            DawgLookup._prepare_local(xs, has_index, sort), partition_mapper, list
        )

    def __init__(
        self,
        df,
        num_partitions=None,
        has_index=False,
        max_records_per_trie=None,
        sort=True,
    ):
        """
        A vocabulary lookup using DAWG trie. For full set of tokens

        :param df: DataFrame with vocabulary
        :param num_partitions: int number of partitions to be used.
            If it is to low, we're likely to see memory failures
        :param has_index: Should we keep index. Works only if 0 <= tokenid <= max-integer-size
        :param max_records_per_trie: int Should we further split partitions
        :param sort: bool Should we resort data before building lookup
        """
        self._sorted = sort

        assert isinstance(df, (DataFrame, Iterable))

        partition_mapper = (
            DawgLookup.simple_partition_mapper
            if max_records_per_trie is None
            else DawgLookup.chunked_partition_mapper(max_records_per_trie)
        )

        if isinstance(df, DataFrame):
            lookups = DawgLookup._lookup_from_spark(
                df, num_partitions, has_index, sort, partition_mapper
            )
        else:
            lookups = DawgLookup._lookup_from_local(
                df, has_index, sort, partition_mapper
            )

        self._lower_bounds, self._upper_bounds, self._lookups = (
            zip(*lookups) if lookups else ((), (), ())
        )
        self._can_bisect = all(
            prev_max < next_min
            for prev_max, next_min in zip(self._upper_bounds, self._lower_bounds[1:])
        )

    def __contains__(self, x):
        """
        """
        # Empty lookup contains nothing
        if not self._lookups:
            return False

        # Data sorted or non-overlaping bounds
        elif self._can_bisect:
            i = bisect.bisect_right(self._lower_bounds, x) - 1
            return False if i < 0 else x in self._lookups[i]

        # Data not sorted and overlapping bounds
        else:
            return x is not None and any(x in lookup for lookup in self._lookups)

    def save(self, path):
        import pickle

        with open(path, "wb") as fw:
            pickle.dump(self, fw)

    @staticmethod
    def load(path):
        import pickle

        with open(path, "rb") as fr:
            return pickle.load(fr)
