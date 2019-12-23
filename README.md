# VSpace

The following package requires following dependencies

- [NLTK](https://www.nltk.org/)
- [toolz](https://github.com/pytoolz/toolz)
- [DAWG](https://github.com/pytries/DAWG)
- PySpark=2.4

as well as Python 3.7 (older versions are not supported, and Python 3.8 support is available only in Spark 3.0.0 and later - [SPARK-29536](https://issues.apache.org/jira/browse/SPARK-29536))

along with PySpark and related libraries.

In the [`scripts/prepare.sh`](scripts/prepare.sh) contains an example script creating a suitable Ananconda environment. Please make sure that `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` a set properly.

To work properly, this package has to be available for each executor, it is however zip friendly, so you should be able to distribute it using `--py-files` / `sparkContext.addPyFile` or equivalent.

You can also build a standard egg file (should be suitable for `--py-files`)

```
python setup.py bdist_egg
```

or wheel:

```
python setup.py bdist_wheel
```

or source distribution:

```
python setup.py sdist
```

You also have to ensure that package dependencies are available on all nodes. Depending on the configuration you might be able to use conda env packaged by `scripts/prepare.sh`.

Spark logging and other configuration options should be set using standard mechanisms. The cleanest solution is to create job specific config directory (`SPARK_CONF_DIR`) and place there the required config files (most notably `spark-defaults.conf` and `log4j.properties`).


The entrypoint is `vspace-main.py` located in the `bin` directory. If package has been installed one can submit the job:

```
spark-submit --py-files path/to/vspace-X.Y.Z-py3.7.egg $(which vspace-main.py) path/to/job.conf
```

otherwise 

```
spark-submit --py-files path/to/vspace-X.Y.Z-py3.7.egg path/to/vspace/source/bin/vspace-main.py path/to/job.conf
```

All input files (phrases, collections, corpus, index, source-to-subsourcess mapping) should present in `vspace_conf.stagingloc`, as defined in the configuration.
