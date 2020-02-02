import itertools
import os
import time
from typing import List, TextIO
import sys

import click
from loguru import logger
import pandas as pd
import tqdm

TABLES_VERSIONS = ['old', 'new']
OLD_TABLES_OUTPUT_PATH = '/share/costeffectiveness/lbwsg_new/old_tables_pickles'
NEW_TABLES_OUTPUT_PATH = '/share/costeffectiveness/lbwsg_new/new_tables_pickles'
PATHS = {'old': OLD_TABLES_OUTPUT_PATH,
         'new': NEW_TABLES_OUTPUT_PATH}
OLD_TABLES_COMMAND = '/share/costeffectiveness/lbwsg_new/miniconda3/envs/lbwsg_old/bin/make_lbwsg_pickle'
NEW_TABLES_COMMAND = '/share/costeffectiveness/lbwsg_new/miniconda3/envs/lbwsg_new/bin/make_lbwsg_pickle'
COMMANDS = {'old': OLD_TABLES_COMMAND,
            'new': NEW_TABLES_COMMAND}


GBD_ROUND_ID = 5
GBD_REPORTING_LOCATION_SET_ID = 1
GBD_MODEL_RESULTS_LOCATION_SET_ID = 35

MEASURES = ['exposure', 'relative_risk', 'population_attributable_fraction']
MEASURES_SHORT = {'exposure': 'exp',
                  'relative_risk': ' rr',
                  'population_attributable_fraction': 'paf'}

VERSIONS_AND_MEASURES = itertools.product(TABLES_VERSIONS, MEASURES)


@click.command()
def make_lbwsg_pickles():
    configure_logging()
    make_all_pickles()


def make_all_pickles():
    drmaa = get_drmaa()
    locations = get_locations()

    jobs = {}
    with drmaa.Session() as session:
        for version, measure in VERSIONS_AND_MEASURES:
            path = PATHS[version]
            command = COMMANDS[version]
            version_measure_jobs = {}
            for location in locations:
                job_template = session.createJobTemplate()
                job_template.remoteCommand = command
                job_template.args = ['-o', path, '-l', f'"{location}"', '-m', measure]
                job_template.nativeSpecification = (f'-V '
                                                    f'-b y '
                                                    f'-P proj_cost_effect '
                                                    f'-q long.q '
                                                    f'-l fmem=10G '
                                                    f'-l fthread=1 '
                                                    f'-l h_rt=2:00:00 '
                                                    f'-l archive=TRUE '
                                                    f'-N {sanitize_location(location)}_{measure}_pickle')
                job_id = session.runJob(job_template)
                version_measure_jobs[location] = (job_id, drmaa.JobState.UNDETERMINED)
                logger.info(f'Submitted job {job_id} to make {measure} pickle for {location} '
                            f'with the {version} version of tables.')
                session.deleteJobTemplate(job_template)
            jobs[(version, measure)] = version_measure_jobs

        logger.info('Entering monitoring loop.')
        logger.info('-------------------------')
        logger.info('')

        progress_bars = {}
        counts = {}
        for idx, (version, measure) in enumerate(VERSIONS_AND_MEASURES):
            pbar_name = f'{MEASURES_SHORT[measure]}_{version}'
            progress_bars[(version, measure)] = tqdm.tqdm(total=len(locations), desc=pbar_name, position=idx)
            counts[(version, measure)] = 0

        finished = [drmaa.JobState.DONE, drmaa.JobState.FAILED]

        while any([job[1] not in finished for job in jobs.values()]):
            time.sleep(10)
            for version, measure in VERSIONS_AND_MEASURES:
                for location, (job_id, status) in jobs[(version, measure)].items():
                    jobs[(version, measure)][location] = (job_id, session.jobStatus(job_id))

            for version, measure in VERSIONS_AND_MEASURES:
                version_measure_jobs = jobs[(version, measure)]
                old_count = counts[(version, measure)]
                new_count = len([job for job in version_measure_jobs.values() if job[1] in finished])
                progress_bars[(version, measure)].update(new_count - old_count)
                counts[(version, measure)] = new_count

    logger.info('**Done**')


def get_drmaa():
    try:
        import drmaa
    except (RuntimeError, OSError):
        if 'SGE_CLUSTER_NAME' in os.environ:
            sge_cluster_name = os.environ['SGE_CLUSTER_NAME']
            if sge_cluster_name == "cluster":  # new cluster
                os.environ['DRMAA_LIBRARY_PATH'] = '/opt/sge/lib/lx-amd64/libdrmaa.so'
            else:  # old cluster - dev or prod
                os.environ['DRMAA_LIBRARY_PATH'] = f'/usr/local/UGE-{sge_cluster_name}/lib/lx-amd64/libdrmaa.so'
            import drmaa
        else:
            drmaa = object()
    return drmaa


def add_logging_sink(sink: TextIO, verbose: int, colorize: bool = False, serialize: bool = False):
    """Adds a logging sink to the global process logger.

    Parameters
    ----------
    sink
        Either a file or system file descriptor like ``sys.stdout``.
    verbose
        Verbosity of the logger.
    colorize
        Whether to use the colorization options from :mod:`loguru`.
    serialize
        Whether the logs should be converted to JSON before they're dumped
        to the logging sink.

    """
    message_format = ('<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | '
                      '<cyan>{function}</cyan>:<cyan>{line}</cyan> '
                      '- <level>{message}</level>')
    if verbose == 0:
        logger.add(sink, colorize=colorize, level="WARNING", format=message_format, serialize=serialize)
    elif verbose == 1:
        logger.add(sink, colorize=colorize, level="INFO", format=message_format, serialize=serialize)
    elif verbose >= 2:
        logger.add(sink, colorize=colorize, level="DEBUG", format=message_format, serialize=serialize)


def configure_logging():
    logger.remove(0)  # Clear default configuration
    add_logging_sink(sys.stdout, verbose=2, colorize=True)


def get_locations() -> List[str]:
    from db_queries import get_location_metadata
    reporting = get_location_metadata(location_set_id=GBD_REPORTING_LOCATION_SET_ID, gbd_round_id=GBD_ROUND_ID)
    reporting = reporting.filter(["location_name"])
    model_results = get_location_metadata(location_set_id=GBD_MODEL_RESULTS_LOCATION_SET_ID, gbd_round_id=GBD_ROUND_ID)
    model_results = model_results.filter(["location_name"])
    locations = pd.concat([reporting, model_results], ignore_index=True).drop_duplicates()
    return locations.location_name.to_list()


def sanitize_location(location: str):
    """Cleans up location formatting for writing and reading from file names.

    Parameters
    ----------
    location
        The unsanitized location name.

    Returns
    -------
        The sanitized location name (lower-case with white-space and
        special characters removed.

    """
    # FIXME: Should make this a reversible transformation.
    return location.replace(" ", "_").replace("'", "_").lower()
