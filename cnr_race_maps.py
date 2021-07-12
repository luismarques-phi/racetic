import boto3
from botocore import UNSIGNED
from botocore.config import Config

from process.edc_action import EDCAction
from process.edc_client import EDCClient
from process.product_source import FileSystemSource
from race_logger import RaceLogger

# collection: dev_lm_racetic
# bucket: dev-lm-racetic


if __name__ == '__main__':

    logger = RaceLogger(name="main")
    root_path = '/home/dev/PycharmProjects/racetic/WIP/CNR'
    filter_extension = '.tif'
    filter_contains = 'tsmnn'
    target_path = '/home/dev/PycharmProjects/racetic/WIP/completed'
    # indicator_key = 'cnr_race_chla_maps'
    indicator_key = 'cnr_race_tsmnn_maps'

    # default
    aws_access_key_id = '7BLSDLJZPDXNGD7IWR2T'
    aws_secret_access_key = "PPUXp4K5EikprmBg2ZBDybb6jSGGVrmJWmPTFCFm"
    region_name = None
    aws_endpoint = 'https://obs.eu-de.otc.t-systems.com'

    collection_id = "9ad17da3-5877-4d14-8141-9809ab52010f"
    band = "tsmnn"
    folder = "DEV-N3-tsmnn"

    aws_bucket = 'dev-lm-racetic'

    if folder[-1] != '/':
        folder += '/'

    # # TODO DELETE
    # chl_collection_id = "fa331cca-5be2-4736-8030-793af55c0ef1"
    # chl_band = "chl"
    # chl_folder = "N3"
    # tsmnn_collection_id = "45ce0fb2-fdaf-481e-b834-f728a8677e59"
    # tsmnn_band = "tsmnn"
    # tsmnn_folder = "N3-tsmnn"
    # times = ""
    # search_dir = "CNR"
    # file_dir = "/sftp/CNR/incoming/race/"
    # tmp_data_dir = "/home/linux/scripts/CNR/maps/"
    # for fileTocopy in $(cat ${list_to_update})
    # do
    # 	sudo cp ${file_dir}$fileTocopy ${tmp_data_dir}
    # 	sudo chown linux:linux ${tmp_data_dir}$fileTocopy
    # done

    # contains the list CNR_ftp_chl.txt, is the path to this file ftp_chl_source

    resource = boto3.resource('s3', config=Config(signature_version=UNSIGNED), region_name=region_name,
                              aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                              endpoint_url=aws_endpoint)

    token_url = 'https://services.sentinel-hub.com/oauth/token'
    client_id = '890744f6-6cef-4c1f-b436-a054bbbc7b9c'
    client_secret = 'h8~F-GgLa.e5PNo+i.x_:<8}BHgkqj1%qyA5Ei#2'
    base_url = "https://shservices.mundiwebservices.com/api/v1"

    edc_client = EDCClient(client_id, client_secret, token_url, base_url)

    try:
        action = EDCAction(aws_bucket=aws_bucket,
                           aws_session=resource,
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

    logger.info(f'START {indicator_key=} files_to_process={len(files_to_process)}')

    for file in files_to_process:
        logger.info(f'IMPORTING_START {indicator_key=} {file.id=}')
        success, message = action.execute(file)
        logger.info(f'IMPORTING_END {indicator_key=} {file.id=} {success=} {message=}')
        # os.rename(os.path.join(file.root_location, file.id), os.path.join(target_path, file.id))

    logger.info(f'END {indicator_key=}')

    # the processing will be done either called via crontab, or via configuration here, TBD
