import datetime
import os
import re
from dataclasses import dataclass, field
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from process.action import Action
from process.edc_client import EDCClient
from process.product import Product
from race_logger import RaceLogger


@dataclass(frozen=True, repr=False)
class EDCAction(Action):
    aws_bucket: 'boto3.resources.factory.s3.Bucket'
    edc_client: EDCClient
    edc_collection_id: str
    band: str
    aws_cache: dict = field(default_factory=dict)
    logger: RaceLogger = field(default=RaceLogger(name="AWSAction"))
    aws_key_prefix: Optional[str] = field(default=None)

    def __post_init__(self):
        # HOW TO CREATE BUCKET
        # aws --endpoint https://obs.eu-de.otc.t-systems.com  s3api create-bucket --acl public-read-write --bucket dev-lm-racetic
        # aws --endpoint https://obs.eu-de.otc.t-systems.com  s3api put-bucket-policy --bucket dev-lm-racetic --policy file://policy.json
        # policy.json
        # {
        #    "Statement": [
        #       {
        #          "Effect": "Allow",
        #          "Principal": "*",
        #          "Action": [
        #             "s3:PutObject"
        #             ],
        #          "Resource": [
        #               "arn:aws:s3:::dev-lm-acl",
        #               "arn:aws:s3:::dev-lm-acl/*"
        #           ]
        #       }
        #    ]
        # }

        # Or more generic: "Action": "s3:*",

        if self.aws_key_prefix:
            it_objs = iter(self.aws_bucket.objects.filter(Prefix=self.aws_key_prefix))
        else:
            it_objs = iter(self.aws_bucket.objects.all())
        try:
            self.aws_cache.update({obj.key: obj for obj in it_objs})
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                raise RuntimeError(f"{e.response['Error']['Message']}:{e.response['Error']['BucketName']}") from e
            else:
                raise e

    def execute(self, file: Product) -> (bool, Optional[str]):
        try:
            aws_key = os.path.join(self.aws_key_prefix, file.id)
            if aws_key in self.aws_cache:
                return True, "Duplicated File"
            else:
                self.logger.info(f"START {file.id=}")
                try:
                    self.upload_to_s3_bucket(file)
                except Exception as e:
                    return False, str(e)

                path = f'{self.aws_key_prefix}{file.id.replace(self.band, "(BAND)")}'
                last_date_in_filename = re.findall(r"([0-9]{4}[0-1][0-9][0-3][0-9])", file.id)[-1]
                sensing_time = datetime.datetime.strptime(last_date_in_filename, '%Y%m%d') - datetime.timedelta(
                    days=-3)  # ISO 8601
                answer = self.edc_client.create_tile(self.edc_collection_id, path, sensing_time.isoformat())

                edc_success = answer.ok
                edc_message = answer.text

                if edc_success:
                    tile_id = answer.json()['data']['id']
                    self.logger.info(f'CHECKING STATUS {file.id=} {path=} {self.edc_collection_id=} {tile_id=}')
                    edc_success, edc_message = self.edc_client.wait_for_status(self.edc_collection_id, tile_id)

                return edc_success, edc_message
        except Exception as e:
            self.logger.exception('Failure')
            return False, str(e)

    def upload_to_s3_bucket(self, file):
        path = os.path.join(file.root_location, file.id)
        object_key = os.path.join(self.aws_key_prefix, file.id)
        self.logger.info(f'UPLOADING {file.id=} {path=} {object_key=}')
        try:
            self.aws_bucket.upload_file(path, object_key, ExtraArgs={'ACL': 'bucket-owner-full-control'})
        except ClientError as e:
            self.logger.exception(e.response['Error'])
            raise e
