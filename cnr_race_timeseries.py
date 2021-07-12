import os

from xcube_geodb.core.geodb import GeoDBClient

from process.geodb_action import GeoDBAction
from process.product_source import FileSystemSource
from race_logger import RaceLogger

GEODB_CONFIG_PARAMS = ['GEODB_API_SERVER_PORT', 'GEODB_API_SERVER_URL', 'GEODB_AUTH_AUD', 'GEODB_AUTH_CLIENT_ID',
                       'GEODB_AUTH_CLIENT_SECRET', 'GEODB_AUTH_DOMAIN']

if __name__ == '__main__':

    logger = RaceLogger(name="main")
    database = "eodash_stage"
    table_name = "N3_stage_LM"
    root_path = '/home/dev/PycharmProjects/racetic/WIP/CNR'
    filter_extension = '.csv'
    target_path = '/home/dev/PycharmProjects/racetic/WIP/completed'
    indicator_key = 'cnr_race_n3_timeseries'

    # Assumes GEODB_API_SERVER_PORT, GEODB_API_SERVER_URL, GEODB_AUTH_AUD, GEODB_AUTH_CLIENT_ID, GEODB_AUTH_CLIENT_SECRET, GEODB_AUTH_DOMAIN are present on the environment
    env_keys = os.environ.keys()
    for geodb_config in GEODB_CONFIG_PARAMS:
        if geodb_config not in env_keys:
            logger.fatal(
                f'{geodb_config} not configured. Be sure all parameters are configured in the environmemt:{GEODB_CONFIG_PARAMS}')
            exit(1)

    action = GeoDBAction(database=database, table_name=table_name, geodb=GeoDBClient())

    product_source = FileSystemSource(root_path=root_path, filter_extension=filter_extension)
    files_to_process = product_source.get_files()

    logger.info(f'START {indicator_key=} files_to_process={len(files_to_process)}')

    for file in files_to_process:
        logger.info(f'IMPORTING_START {indicator_key=} {file.id=}')
        success, message = action.execute(file)
        logger.info(f'IMPORTING_END {indicator_key=} {file.id=} {success=} {message=}')
        os.rename(os.path.join(file.root_location, file.id), os.path.join(target_path, file.id))

    logger.info(f'END {indicator_key=}')

    # the processing will be done either called via crontab, or via configuration here, TBD
