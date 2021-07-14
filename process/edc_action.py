import datetime
import os
import re
from dataclasses import dataclass, field
from typing import Optional

from boto3 import Session

from process.action import Action
from process.edc_client import EDCClient
from process.product import Product
from race_logger import RaceLogger


@dataclass(frozen=True, repr=False)
class EDCAction(Action):
    aws_bucket: str
    aws_session: Session
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
        #          "Action": "s3:*",
        #          "Resource": "arn:aws:s3:::dev-lm-racetic"
        #       },
        #       {
        #          "Effect": "Allow",
        #          "Principal": "*",
        #          "Action": "s3:*",
        #          "Resource": "arn:aws:s3:::dev-lm-racetic/*"
        #       }
        #    ]
        # }

        bucket = self.aws_session.Bucket(self.aws_bucket)
        if self.aws_key_prefix:
            it_objs = iter(bucket.objects.filter(Prefix=self.aws_key_prefix))
        else:
            it_objs = iter(bucket.objects.all())
        try:
            self.aws_cache.update({obj.key: obj for obj in it_objs})
        except self.aws_session.meta.client.exceptions.NoSuchBucket as e:
            raise RuntimeError(f"The bucket {self.aws_bucket} does not exist.") from e

    def execute(self, file: Product) -> (bool, Optional[str]):
        try:
            aws_key = os.path.join(self.aws_key_prefix, file.id)
            if aws_key in self.aws_cache:
                return True, "Duplicated File"
            else:
                self.logger.info(f"START {file.id=}")
                self.upload_to_s3_bucket(file)

                path = f'{self.aws_key_prefix}{file.id.replace(self.band, "(BAND)")}'
                last_date_in_filename = re.findall(r"([0-9]{4}[0-1][0-9][0-3][0-9])", file.id)[-1]
                sensing_time = datetime.datetime.strptime(last_date_in_filename, '%Y%m%d') - datetime.timedelta(days=-3)  # ISO 8601
                answer = self.edc_client.create_tile(self.edc_collection_id, path, sensing_time.isoformat())

                return answer.ok, answer.text
        except Exception as e:
            self.logger.exception('Failure')
            return False, str(e)

    def upload_to_s3_bucket(self, file):
        path = os.path.join(file.root_location, file.id)
        bucket = self.aws_bucket
        object_key = os.path.join(self.aws_key_prefix, file.id)
        self.logger.info(f'UPLOADING {file.id=} {path=} {bucket=} {object_key=}')
        # TODO Manage Failure
        with open(path, 'rb') as data:
            self.aws_session.meta.client.upload_fileobj(data, bucket, object_key,
                                                        ExtraArgs={'ACL': 'public-read'})
