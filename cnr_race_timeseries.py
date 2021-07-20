import os
from argparse import ArgumentParser
from configparser import RawConfigParser

from xcube_geodb.core.geodb import GeoDBClient

from process.geodb_action import GeoDBAction
from process.product_source import FileSystemSource
from race_logger import RaceLogger

if __name__ == '__main__':

    # --path /home/dev/PycharmProjects/racetic/WIP/CNR --type .csv --database eodash_stage --table N3_stage_LM --archive /home/dev/PycharmProjects/racetic/WIP/completed --indicator cnr_race_n3_timeseries --config /home/dev/PycharmProjects/racetic/timeseries_cred.cfg

    logger = RaceLogger(name="main")
    parser = ArgumentParser()

    parser.add_argument("--path", type=str, help="Specify the path to directory to be processed", required=True)
    parser.add_argument("--type", type=str, help="Specify the type(extension) of files to be processed",
                        required=True)
    parser.add_argument("--database", type=str, help='Specify the GeoDB Database to use', required=True)
    parser.add_argument("--table", type=str, help='Specify the GeoDB table to use', required=True)
    parser.add_argument("--archive", type=str,
                        help='Specify the target folder, for where the files move once processed', required=True)
    parser.add_argument("--indicator", type=str,
                        help="Specify the key for which the monitoring system will group the results", required=True)

    parser.add_argument("--config", type=str, help="Specify path to AWS and EDC configurations", required=True)

    args = parser.parse_args()
    config = RawConfigParser()
    config.read_file(open(args.config))

    database = args.database
    table_name = args.table
    root_path = args.path
    filter_extension = args.type
    target_path = args.archive
    indicator_key = args.indicator

    os.environ["GEODB_API_SERVER_PORT"] = config.get("GEODB", "API_SERVER_PORT")
    os.environ["GEODB_API_SERVER_URL"] = config.get("GEODB", "API_SERVER_URL")
    os.environ["GEODB_AUTH_AUD"] = config.get("GEODB", "AUTH_AUD")
    os.environ["GEODB_AUTH_CLIENT_ID"] = config.get("GEODB", "AUTH_CLIENT_ID")
    os.environ["GEODB_AUTH_CLIENT_SECRET"] = config.get("GEODB", "AUTH_CLIENT_SECRET")
    os.environ["GEODB_AUTH_DOMAIN"] = config.get("GEODB", "AUTH_DOMAIN")

    action = GeoDBAction(database=database, table_name=table_name, geodb=GeoDBClient())

    product_source = FileSystemSource(root_path=root_path, filter_extension=filter_extension)
    files_to_process = product_source.get_files()

    count_ok = 0
    count_nok = 0

    logger.info(f'START {indicator_key=} files_to_process={len(files_to_process)}')

    for file in files_to_process:
        logger.info(f'IMPORTING_START {indicator_key=} {file.id=}')
        success, message = action.execute(file)
        logger.info(f'IMPORTING_END {indicator_key=} {file.id=} {success=} {message=}')
        if success:
            count_ok += 1
            os.rename(os.path.join(file.root_location, file.id), os.path.join(target_path, file.id))
        else:
            count_nok += 1

    logger.info(f'END {indicator_key=} files_to_process={len(files_to_process)} ok={count_ok} nok={count_nok}')
