from __future__ import annotations

import os
import json
import logging
from typing import Dict, Optional

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.io.filesystems import FileSystems

logger = logging.getLogger(__name__)


class BigflowOptions(PipelineOptions):
    # apache_beam scans all subclasses of `PipelineOptions`
    # to get list of all available options.

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--bigflow-env",
            help="Bigflow environment",
        )
        parser.add_argument(
            "--bigflow-workflow",
            help="Bigflow workflow id",
        )
        parser.add_argument(
            "--bigflow-jobid",
            help="Bigflow Job id",
        )


def get_pipeline_options() -> Optional[Dict]:
    """Get current pipeline options from driver/worker process."""

    logger.debug("Search for pipeline options")

    if RuntimeValueProvider.runtime_options is not None:
        # Worker bootstrapper called 'RuntiemValueProvider.set_options'
        logger.debug("Running inside worker - return runtime_options")
        return RuntimeValueProvider.runtime_options

    pipeline_options_env = os.environ.get('PIPELINE_OPTIONS')
    if pipeline_options_env:
        # looks like we are running inside sdk_harness_worker,
        # but RuntimeValueProvider is not initialized (yet?)
        # this known to happen at least for 2.28

        logger.info("Found PIPELINE_OPTIONS - try to parse json")
        raw = data = None
        try:
            data = json.loads(pipeline_options_env)
            options = data['options']
        except ValueError:
            logger.exception("Invalid PIPELINE_OPTIONS json: %r", raw)
        except KeyError:
            logger.exception("PIPELINE_OPTIONS doesn't contain 'options': %r", data)
        else:
            logger.info("Parsed options from PIPELINE_OPTIONS")
            return options

    if FileSystems._pipeline_options is not None:
        # DirectRunner is active - we'd have a ling to instance of PipelineOptions
        opts = FileSystems._pipeline_options
        if isinstance(opts, PipelineOptions):
            opts = opts.get_all_options()
        return opts

    return None
