#!/home/deepcompute/vspace/anaconda/bin/python

import argparse
import logging
import logging.config
from time import time

import vspace
from vspace.pipelines.base import main
from vspace.utils.text import normalize
from vspace.lookups.dawg import DawgLookup


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="VSpace")
    parser.add_argument(
        "vspace_config",
        metavar="vspace-config",
        type=str,
        help="A path to configuraton file",
    )
    parser.add_argument(
        "--logging-config",
        metavar="logging-config",
        dest="logging_config",
        type=str,
        help="A path to Python logging configuraton file",
        required=False,
    )
    args = parser.parse_args()

    if args.logging_config:
        logging.config.fileConfig(args.logging_config)
    else:
        logging.basicConfig(
            filename= 'logs/' + args.vspace_config + '-vspace.log',
            level="INFO",
            format="%(levelname)s %(asctime)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    st = time()
    logging.info("Using vspace {}".format(vspace.__version__))

    main(config_path=args.vspace_config)
    logging.info("Time taken %.2f hrs" %((st-time())/3600))
