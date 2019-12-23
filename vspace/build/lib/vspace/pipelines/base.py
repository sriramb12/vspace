import argparse
from collections import Counter
import logging
from operator import itemgetter
import pathlib
import re
import sys


if sys.version_info < (3,):
    from ConfigParser import ConfigParser
else:
    from configparser import ConfigParser

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# Third party libraries not present at the moment
from toolz.curried import filter, frequencies, map, mapcat, pipe
from toolz.functoolz import curry
import dawg
from nltk.util import everygrams

import vspace
from vspace.utils.text import normalize
from vspace.lookups.dawg import DawgLookup

RECORD_DELIMITER = "nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword"

DEFAULT_INDEX_SCHEMA = StructType(
    [
        StructField("document_index", LongType()),
        StructField("url", StringType()),
        StructField("subsource", StringType()),
        StructField("year", IntegerType()),
        StructField("meta1", StringType()),  # TODO Fill the name if needed
        StructField("title", StringType()),
        StructField("author", StringType()),
        StructField("meta2", StringType()),
        StructField("meta3", StringType()),
        StructField("meta4", StringType()),
    ]
)

swap = itemgetter(1, 0)


def load_raw_corpus(spark, path, record_delimiter=RECORD_DELIMITER):
    """
    Load corpus file

    :param spark: SparkSession instance.
    :param path: Path to the input file.
    :param record_delimiter: Delimiter to be passed as textinputformat.record.delimiter.
                             Default: RECORD_DELIMITER
    :return: RDD[Tuple[int, str]]
    """
    raw = spark.sparkContext.newAPIHadoopFile(
        path,
        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "org.apache.hadoop.io.Text",
        conf={"textinputformat.record.delimiter": record_delimiter},
    )
    return raw.values().zipWithIndex().map(swap)


def load_index(spark, path, delimiter="\t", schema=DEFAULT_INDEX_SCHEMA):
    """
    Load index file

    :param spark: SparkSession instance.
    :param path: Path to the input file.
    :param delimiter: Delimiter to be used with csv reader
    :param schema: StructType or equivalent DSL str. Deault: DEFAULT_INDEX_SCHEMA
    :return: DataFrame
    """
    return (
        spark.read.format("csv")
        .schema(schema)
        .options(delimiter=delimiter, header="false")
        .load(path)
    )


def load_sources(spark, path, delimiter=" "):
    """
    Load sources mapping file

    :param spark: SparkSession instance.
    :param path: Path to the input file.
    :param delimiter: Delimiter to be used with csv reader
    :return: DataFrame[subsource: string, sources: array<string>]
    """
    return (
        spark.read.format("csv")
        .schema(
            StructType(
                [
                    StructField("source", StringType()),
                    StructField("subsources", StringType()),
                ]
            )
        )
        .options(delimiter=delimiter, header="false")
        .load(path)
        .withColumn("subsource", F.explode(F.split("subsources", ",")))
        .groupby("subsource")
        .agg(F.collect_list("source").alias("sources"))
    )


def tokenize(s):
    """
    A naive tokenizer
    """
    return str.split(s)


def ngram_counts(
    tokens, min_len=1, max_len=None, transform=" ".join, in_vocabulary=lambda _: True
):
    """
    Compute n-gram counts using toolz and Counter

    :param tokens: Iterable[str]
    :param min_len: int Minimum N-Gram size
    :param max_len: int Maximum N-Gram size
    :param transfrom: Callable[[Tuple[str, ...], str]] Function transforming ngram tuple into key
    :param in_vocabulary: Callable[[str], bool] Should token be preserved
    :return: Dict[str, int]
    """
    tokens = list(tokens)
    wc = len(tokens)
    max_len = (max_len if max_len else wc) + 1
    return (
        wc,
        pipe(
            everygrams(tokens, min_len=min_len, max_len=max_len),
            map(transform),
            filter(in_vocabulary),
            frequencies,
        ),
    )


def process_corpus(
    raw_corpus, normalizer=normalize, tokenizer=tokenize, ngram_counter=ngram_counts
):
    """
    :param raw_corpus: RDD[Tuple[int, str]] as returned from load_raw_corpus
    :param normalizer: Callable[[str], str] preprocessing function
    :param tokenizer: Callable[[str], Iterable[str]]
    :param ngram_counter: Callable[[Iterable[str], Dict[str, int]]]
    :return: DataFrame[document_index: bigint, wc: bigint, token_counts: map<string,int>]
    """
    schema = StructType(
        [
            StructField("document_index", LongType()),
            StructField(
                "data",
                StructType(
                    [
                        StructField("wc", LongType()),
                        StructField(
                            "token_counts", MapType(StringType(), IntegerType())
                        ),
                    ]
                ),
            ),
        ]
    )
    normalized = raw_corpus.mapValues(normalize)
    return (
        normalized
        .mapValues(tokenize)
        .mapValues(ngram_counter)
        .toDF(schema)
        .select("document_index", "data.wc", "data.token_counts"),
        normalized
    )


def compute_stats(df, grouping):
    """
    Compute statistics for df, grouped by grouping columns

    :param df: DataFrame
    :param grouping: Union[Iterable[str], Iterable[Column]]
    :return: DataFrame
    """
    return (
        df.select("*", F.explode("token_counts").alias("token", "tf"))
        .groupBy(*grouping)
        .agg(
            F.count("*").alias("document_frequency"),
            F.sum("tf").alias("term_frequency"),
            F.sum("wc").alias("tdsum"),
        )
    )


def combine_corpus_with_sources(corpus, index, sources):
    """
    Merge index and sources and then merge result with corpusSharder

    :param corpus: DataFrame as returned from process_corpus
    :param index: DataFrame as returned from load_index
    :param sources: DataFrame as returned from load_sources
    :return DataFrame:
    """

    source_document_map = (
        index.select("document_index", "subsource")
        .join(F.broadcast(sources), "subsource")
        .drop("subsource")
    )

    corpus_with_sources = corpus.join(source_document_map, "document_index").withColumn(
        "source", F.explode("sources")
    )

    return corpus_with_sources


def try_decode(raw, encoding="utf-8"):
    """
    Try to encode by bytes using given encoding

    :param raw: bytes
    :param encoding: str
    :return: Generator[str, None, None]
    """
    try:
        yield (
            raw.decode(encoding)
            .replace("\x00", "")
            .replace("\x01", "")
            .replace("\t", "")
        )
    except UnicodeDecodeError:
        pass


def load_and_decode(spark, path, encoding):
    """
    Try to load data without decoding and try to decode explictly.

    This is used as workaround for issues with encoding in phrases and collections

    :param spark: SparkSession
    :param path: str
    :param encoding: str
    :return: RDD[str]
    """
    return spark.sparkContext.textFile(path, use_unicode=False).flatMap(
        lambda raw: try_decode(raw, encoding)
    )


def undersores_to_spaces(col):
    """
    Replace undersores with spaces

    :param col: Union[str, Column] A column or a name of a column
    """
    return F.translate(col, "_", " ")


def load_phrases(spark, path, encoding):
    """
    Load phrases file

    :param spark: SparkSession
    :param path: str
    :param encoding: str
    :return: DataFrame
    """
    return spark.createDataFrame(
        load_and_decode(spark, path, encoding), StringType()
    ).select(undersores_to_spaces(F.split("value", " ").getItem(0)).alias("token"))


def load_collections(spark, path, encoding):
    """
    Load collections file

    :param spark: SparkSession
    :param path: str
    :param encoding: str
    :return: DataFrame
    """
    return spark.createDataFrame(
        load_and_decode(spark, path, encoding), StringType()
    ).select(undersores_to_spaces("value").alias("token"))


def main(config_path):
    conf = ConfigParser()
    conf.read(config_path)

    staging_dir = pathlib.PosixPath(conf.get("vspace_conf", "stagingloc"))

    corpus_path = staging_dir / conf.get("vspace_input", "corpus")
    logging.info("conf file %s" %config_path)
    index_path = staging_dir / conf.get("vspace_input", "index2doc")
    sources_path = staging_dir / conf.get("vspace_input", "src2sub")

    collections_path = staging_dir / conf.get("vspace_input", "collections")
    phrases_path = staging_dir / conf.get("vspace_input", "phrases")

    output_path = pathlib.PosixPath(
        conf.get("vspace_conf", "outputFolder", fallback="output")
    )
    source_token_stats_path = output_path / "source_stats"
    global_token_stats_path = output_path / "global_stats"

    vocabulary_path = output_path / "vocabulary"

    with SparkSession.builder.appName(
        "vspace-{}".format(vspace.__version__ + '-' + str(corpus_path))
    ).getOrCreate() as spark:

        logging.info("SparkSession initalized")
        
        logging.info("Spark env")
        sparkEnv = spark.sparkContext.getConf().getAll()
        for i in sparkEnv:
           logging.info(i)

        spark.sparkContext.setJobGroup("vocabulary", "Vocabulary processing")

        # Load vocabulary
        logging.info("Processing vocabulary")

        phrases = load_phrases(spark, phrases_path.as_posix(), "utf-8")
        collections = load_collections(spark, collections_path.as_posix(), "utf-8")

        vocabulary = (
            phrases.union(collections)
            # That is expensive and might not be necessary
            .distinct()
            .withColumn("tokenid", F.monotonically_increasing_id())
            .persist()
        )
        vocabulary.write.format("csv").mode("overwrite").options(
            header="false", delimiter="\t"
        ).save(vocabulary_path.as_posix())

        logging.info("Vocabulary table written")

        spark.sparkContext.setJobGroup("lookup", "Building lookup")

        lookup = spark.sparkContext.broadcast(
            # TODO Parametrize with config
            DawgLookup(vocabulary, num_partitions=20)
        )

        vocabulary.unpersist()

        logging.info("Vocabulary lookup built")

        spark.sparkContext.setJobGroup(
            "load corpus", "load_raw_corpus"
        )


        DOCID_PATTERN = re.compile("^nferdoccount_[0-9]+$")

        corpus, normalized = process_corpus(
            load_raw_corpus(spark, corpus_path.as_posix()),
            ngram_counter=curry(
                ngram_counts,
                max_len=conf.getint("vspace_conf", "maxngrams", fallback=3),
                in_vocabulary=lambda s: (
                    not DOCID_PATTERN.match(s)  # Not an identifier
                    and (" " not in s or s in lookup.value)  # Unigram or in lookup
                ),
            ),
        )
        corpus.persist()
        logging.info("saving normalized corpus")

        normalized_path = output_path / "normalized"
        normalized.saveAsTextFile(normalized_path.as_posix())
        logging.info("saved normalized corpus %s" %normalized_path.as_posix())

        logging.info("Corpus defined")

        spark.sparkContext.setJobGroup(
            "load index", "load_index"
        )

        index = load_index(spark, index_path.as_posix())

        spark.sparkContext.setJobGroup(
            "load sources", "load_sources"
        )

        sources = load_sources(spark, sources_path.as_posix())

        spark.sparkContext.setJobGroup(
            "combine  sources corpus", "combine_corpus_with_sources"
        )

        logging.info("Sources and indices loaded")
        corpus_with_sources = combine_corpus_with_sources(corpus, index, sources)

        num_partitions = conf.get("vspace_conf", "splits", fallback=None)
        if num_partitions:
            spark.conf.set("spark.sql.shuffle.partitions", num_partitions)

        source_token_stats = compute_stats(corpus_with_sources, ["token", "source"])
        global_token_stats = compute_stats(corpus, ["token"])

        source_token_stats.write.mode("overwrite").options(
            header="false", delimiter="\t"
        ).partitionBy("source").csv(source_token_stats_path.as_posix())

        logging.info("Source-token statistics written")

        spark.sparkContext.setJobGroup(
            "corpus-global-sats", "Global corpus stats processing"
        )

        global_token_stats.write.mode("overwrite").options(
            header="false", delimiter="\t"
        ).csv(global_token_stats_path.as_posix())

        logging.info("Global token statistics written")

    logging.info("SparkSession stopped")
