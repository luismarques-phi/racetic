import os
from argparse import ArgumentParser
from configparser import RawConfigParser

import boto3

from process.edc_action import EDCAction
from process.edc_client import EDCClient
from process.product_source import FileSystemSource
from race_logger import RaceLogger

if __name__ == '__main__':

    # --path /home/dev/PycharmProjects/racetic/WIP/CNR --type .tif --filter tsmnn --archive /home/dev/PycharmProjects/racetic/WIP/completed --indicator cnr_race_tsmnn_maps --config /home/dev/PycharmProjects/racetic/maps_cred.cfg --edc_collection_id 9ad17da3-5877-4d14-8141-9809ab52010f --band tsmnn --aws_folder DEV-N3-tsmnn --aws_bucket dev-lm-racetic

    logger = RaceLogger(name="main")
    parser = ArgumentParser()

    parser.add_argument("--path", type=str, help="Specify the path to directory to be processed", required=True)
    parser.add_argument("--type", type=str, help="Specify the type(extension) of files to be processed",
                        required=True)
    parser.add_argument("--filter", type=str, help='Specify the filter for filenames', required=False)
    parser.add_argument("--archive", type=str,
                        help='Specify the target folder, for where the files move once processed', required=True)
    parser.add_argument("--indicator", type=str,
                        help="Specify the key for which the monitoring system will group the results", required=True)
    parser.add_argument("--edc_collection_id", type=str, help="Specify EDC Collection to where files are ingested",
                        required=True)
    parser.add_argument("--band", type=str, help="Specify band found in the filename", required=True)
    parser.add_argument("--aws_folder", type=str, help="Specify AWS folder to be used inside bucket", required=True)
    parser.add_argument("--aws_bucket", type=str, help="Specify AWS bucket to be used", required=True)
    parser.add_argument("--config", type=str, help="Specify path to AWS and EDC configurations", required=True)

    args = parser.parse_args()
    config = RawConfigParser()
    config.read_file(open(args.config))

    root_path = os.path.abspath(args.path)
    filter_extension = args.type
    filter_contains = args.filter
    target_path = os.path.abspath(args.archive)
    indicator_key = args.indicator

    # default
    aws_access_key_id = config.get("AWS", "ACCESS_KEY_ID")
    aws_secret_access_key = config.get("AWS", "SECRET_ACCESS_KEY")
    region_name = config.get("AWS", "REGION_NAME")
    if not region_name:
        region_name = None
    aws_endpoint = config.get("AWS", "ENDPOINT_URL")
    if not aws_endpoint:
        aws_endpoint = None

    token_url = config.get("EDC", "TOKEN_URL")
    client_id = config.get("EDC", "CLIENT_ID")
    client_secret = config.get("EDC", "CLIENT_SECRET")
    base_url = config.get("EDC", "BASE_URL")

    collection_id = args.edc_collection_id
    band = args.band
    folder = args.aws_folder

    if folder[-1] != '/':
        folder += '/'

    resource = boto3.resource('s3', region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key,
                              endpoint_url=aws_endpoint)

    aws_bucket = resource.Bucket(args.aws_bucket)

    edc_client = EDCClient(client_id, client_secret, token_url, base_url)

    try:
        action = EDCAction(aws_bucket=aws_bucket,
                           aws_key_prefix=folder,
                           band=band,
                           edc_client=edc_client,
                           edc_collection_id=collection_id)
    except Exception as e:
        logger.exception("Failed Action")
        exit(1)

    product_source = FileSystemSource(root_path=root_path, filter_extension=filter_extension,
                                      filter_contains=filter_contains)
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
