import os.path
from dataclasses import dataclass, field
from typing import Optional

import geopandas
import pandas
from shapely.geometry import Point
from xcube_geodb.core.geodb import GeoDBClient

from process.action import Action
from process.product import Product
from race_logger import RaceLogger


def prepare_column_names(pd):
    pd.columns = [column.split("[")[0].split("(")[0].lstrip().rstrip().lower().replace(" ", "_").replace("-", "_") for
                  column in pd.columns]
    return pd


@dataclass(frozen=True)
class GeoDBAction(Action):
    database: str
    table_name: str
    geodb: GeoDBClient
    logger: RaceLogger = field(default=RaceLogger(name="GeoDBAction"))

    def execute(self, file: Product) -> (bool, Optional[str]):

        self.logger.debug(f'Running on {self.database=} {self.table_name=}')
        if file.extension != ".csv":
            return False, f"{file.extension=} not valid"

        message = None
        success = False
        part = 0
        for raw_data in pandas.read_csv(os.path.join(file.root_location, file.id), chunksize=100, delimiter=",",
                                        engine='python', encoding='utf-8'):
            # .fillna("/")

            part += 1
            frames_to_send = [(0, raw_data.fillna("/"))]

            while frames_to_send:
                self.logger.debug(f'frames_to_send remaining:{len(frames_to_send)}')
                split, pd_data = frames_to_send.pop()
                if not len(pd_data):
                    continue

                # TODO CHECK IF NEEDED Prepare column names
                prepare_column_names(pd_data)

                # region_data['geometry'] = gpd.GeoSeries.from_wkt(region_data['geometry'])
                # gpd_data = gpd.GeoDataFrame(pd_data, geometry='geometry')
                points = self.update_geometry(pd_data)
                # is gpd_data necessary, isn't the pd_data enough and doing the same thing?
                gpd_data = geopandas.GeoDataFrame(pd_data, geometry=points)
                gpd_data.crs = "epsg:4326"

                self.logger.debug(f'START {file.id=} {part=} {split=}')
                # self.logger.debug(f'{pd_data=}')
                # self.logger.debug(f'{gpd_data=}')

                try:
                    # TODO CHECK WHAT IT RETURNS AND IF IS WORTH IT TO LOG IT
                    tmp_result = self.geodb.insert_into_collection(self.table_name, gpd_data, crs=4326,
                                                                   database=self.database)
                    success = True
                    message = tmp_result.message
                except Exception as ex:
                    ...  # TODO HERE
                    # print(
                    #     f'END {os.path.basename(data_file)} part:{part} split:{split} subsplit {split} {from_df_country} {from_df_region}:{to_df_region} NOK')
                    # print(ex)
                    # traceback.print_exc()
                    # if len(pd_data) == 1:
                    #     df_date = pd_data['date'].iloc[0]
                    #     print(f'ERROR WITH LINE FROM    {from_df_country}   {from_df_region} {df_date}')
                    #     # print(pd_data)
                    #
                    #     sys.stdout.flush()
                    #     sys.stderr.flush()
                    #
                    #     all_good = False
                    # else:
                    #     for new_frame in split_dataframe(pd_data, int(len(pd_data) / 10)):
                    #         frames_to_send.append((split + 1, new_frame))
                    # time.sleep(15)
                finally:
                    self.logger.debug(f'END {file.id=} {part=} {split=} {success=} {message=}')
        # if all_good:
        #     os.remove(data_file)
        #     os.remove(region_file)

        return success, message

    def update_geometry(self, pd_data):
        # We switch the coordiante order, since geopandas always use (x,y)
        points = pd_data.apply(lambda row: Point(reversed([float(coord) for coord in row.aoi.split(",")])),
                               axis=1)  # We switch the coordiante order, since geopandas always use (x,y)
        return points
