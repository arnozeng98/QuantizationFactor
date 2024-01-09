__author__ = 'Lingsong Zeng'

from datetime import datetime
from tqdm import tqdm
import pandas as pd
import os
from function.read_data_config import ReadConfig
from function.read_index_columns import ReadIndex


class FloatVolume:
    """
    Read data from wind.ASHARECAPITALIZATION and convert to following format

        WIND_CODE: Wind代码  -> column
        CHANGE_DT: 变动日期  -> index

         Factor      | Comment
        -------------|---------------
         FLOAT_A_SHR | 流通A股 (万股)

                      | 600373.SH | 300557.SZ | 000416.SZ | ...
            ----------|-----------|-----------|-----------|-----
             20160104 |118568.1515|    nan    |53161.02540|
             20160105 |118568.1515|    nan    |53161.02540| ...
             20160106 |118568.1515|    nan    |53161.02540|
               ...    |    ...    |    ...    |    ...    |
    """

    def __init__(self):
        """Initialize the class"""

        ri = ReadIndex()
        self.index = ri.read_index()     # For the date list we constructed before
        self.column = ri.read_columns()  # For the stock list we constructed before

        rc = ReadConfig()
        self.db = rc.read_wind_mysql()

        data_path = rc.read_factor_path()  # for reading data from local and outputting data to local
        self.data_path = os.path.abspath(os.path.join(data_path, 'ASHARECAPITALIZATION'))
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

    def read_data_before_begin_date(self, begin_date: str) -> pd.DataFrame:
        """
        Read data one date before the begin_date
        :param begin_date: str
        :return: pd.DataFrame
        """

        sql = """
        SELECT f.WIND_CODE, f.CHANGE_DT, f.FLOAT_A_SHR
        FROM (
            SELECT WIND_CODE, max(CHANGE_DT) as last_CHANGE_DT
            FROM wind.ASHARECAPITALIZATION
            WHERE CHANGE_DT <= {} AND WIND_CODE NOT REGEXP '^[a-zA-Z]'
            group by WIND_CODE
        ) AS x inner join wind.ASHARECAPITALIZATION as f on f.WIND_CODE = x.WIND_CODE and f.CHANGE_DT = x.last_CHANGE_DT
        WHERE FLOAT_A_SHR > 0;
        """.format(begin_date)

        return pd.read_sql(sql, self.db)

    def read_data_after_begin_date(self, begin_date: str) -> pd.DataFrame:
        """
        Read data from begin_date
        :param begin_date: str
        :return: pd.DataFrame
        """

        sql = """
        SELECT WIND_CODE, CHANGE_DT, FLOAT_A_SHR
        FROM wind.ASHARECAPITALIZATION
        WHERE CHANGE_DT >= {} AND WIND_CODE NOT REGEXP '^[a-zA-Z]' AND FLOAT_A_SHR > 0;
        """.format(begin_date)

        return pd.read_sql(sql, self.db)

    def read_data(self, begin_date: str = '20160101') -> pd.DataFrame:
        """
        Read wind.ASHARECAPITALIZATION data from database
        :param begin_date: str (default '20160101')
        :return: DataFrame
        """

        data_a = self.read_data_before_begin_date(begin_date)
        data_b = self.read_data_after_begin_date(begin_date)
        return data_a.append(data_b, ignore_index=True)

    def read_exist_data(self) -> object:
        """
        Read exist capitalization data from local
        :return: object
        """

        data_path = os.path.abspath(os.path.join(self.data_path, 'float_volume.hdf5'))
        return pd.read_hdf(data_path, key='float_volume')

    def rewrite_data(self):
        """Re-format the data from wind.ASHARECAPITALIZATION"""

        # float_a_shr from wind.ASHARECAPITALIZATION
        float_a_shr = self.read_data()

        main_dict = {}
        stock_dict = {}

        # main_dict = {
        #
        #     '600373.SH': {'20160104': 118568.1515,
        #                   '20160105': 118568.1515,    -> stock_dict
        #                   '20160106': 118568.1515,
        #                   ......}
        #
        #     '300557.SZ': {'20161101': 1400.0,
        #                   '20161102': 1400.0,         -> stock_dict
        #                   '20161103': 1400.0,
        #                   ......}
        #
        #                   ......                   }

        float_a_shr = float_a_shr.sort_values(['WIND_CODE', 'CHANGE_DT']).to_dict('records')
        pre_record_ = float_a_shr[0]  # previous object in loop, for defining the date interval
        index_iter = iter(self.index)

        for record_ in tqdm(float_a_shr[1:]):

            # loop content:
            #
            # [{'WIND_CODE': '000301.SZ', 'CHANGE_DT': '20090611', 'FLOAT_A_SHR': 121823.6445},  -> pre_record_
            #  {'WIND_CODE': '000301.SZ', 'CHANGE_DT': '20180903', 'FLOAT_A_SHR': 121823.6445},  -> record_
            #  {'WIND_CODE': '000301.SZ', 'CHANGE_DT': '20200630', 'FLOAT_A_SHR': 121820.0945},
            #   ......                                                                          ]

            code_1, date_1, value_1 = pre_record_.values()
            code_2, date_2, value_2 = record_.values()

            if code_1 != code_2:  # if pre_record_ and record_ are not the same stock

                # fill the rest by using the last value we have
                for index in index_iter:
                    stock_dict[index] = value_1

                # insert stock_dict into main_dict then empty stock_dict
                main_dict[code_1] = stock_dict.copy()
                stock_dict.clear()
                index_iter = iter(self.index)  # reset iterator

            else:
                # loop through index, insert value_1 as its value if index in [date_1, date_2)
                for index in index_iter:
                    if index < date_1:
                        continue
                    elif index < date_2:
                        stock_dict[index] = value_1
                    elif index == date_2:
                        stock_dict[index] = value_2
                    else:
                        # else we reset iterator from the break point INCLUDING the index at break point, then break
                        index_iter = iter(self.index[self.index.index(index):])
                        break

            pre_record_ = record_

        # Since we are inserting pre_record_ everytime, there is one more record_ left to be insert
        for index in index_iter:
            stock_dict[index] = pre_record_['FLOAT_A_SHR']
        main_dict[pre_record_['WIND_CODE']] = stock_dict.copy()

        main_dict = pd.DataFrame(main_dict, columns=self.column)

        # store data as hdf5 file
        data_path = os.path.abspath(os.path.join(self.data_path, 'float_volume.hdf5'))
        main_dict.to_hdf(data_path, key='float_volume')
        print("\nFinished rewriting data")

    def update_data(self):
        """
        Update local float_volume data generated before
        Note:
            1. if there is no new data to update, we end the function directly
            2. we start reading from the first date of new data, so we do not pull superfluous data from database
        """

        # exist_data from local
        exist_data = self.read_exist_data()
        new_date = sorted(list(set(self.index) - set(exist_data.index)))

        # len(new_date) == 0 means no need to update so we end the function
        if len(new_date) == 0:
            print("\nData is already updated.")
            return None  # function ends here

        new_data = self.read_data(new_date[0])

        update = {}
        stock_dict = {}

        index_iter = iter(new_date)  # we don't have to loop through the entire index but just new dates
        new_data = new_data.sort_values(['WIND_CODE', 'CHANGE_DT']).to_dict('records')
        pre_record_ = new_data[0]  # previous object in loop, for defining the date interval

        for record_ in tqdm(new_data[1:]):

            code_1, date_1, value_1 = pre_record_.values()
            code_2, date_2, value_2 = record_.values()

            if code_1 != code_2:  # if pre_record_ and record_ are not the same stock

                # fill the rest by using the last value we have
                for index in index_iter:
                    stock_dict[index] = value_1

                # insert stock_dict into main_dict then empty stock_dict
                update[code_1] = stock_dict.copy()
                stock_dict.clear()
                index_iter = iter(self.index)  # reset iterator

            else:
                # loop through index, insert value_1 as its value if index in [date_1, date_2)
                for index in index_iter:
                    if index < date_1:
                        continue
                    elif index < date_2:
                        stock_dict[index] = value_1
                    elif index == date_2:
                        stock_dict[index] = value_2
                    else:
                        # else we reset iterator from the break point INCLUDING the index at break point, then break
                        index_iter = iter(self.index[self.index.index(index):])
                        break

            pre_record_ = record_

        # Since we are inserting pre_record_ everytime, there is one more record_ left to be insert
        for index in index_iter:
            stock_dict[index] = pre_record_['FLOAT_A_SHR']

        update[pre_record_['WIND_CODE']] = stock_dict.copy()
        update = pd.DataFrame(update, columns=self.column)
        exist_data = exist_data.append(update)

        # store data as hdf5 file
        data_path = os.path.abspath(os.path.join(self.data_path, 'float_volume.hdf5'))
        exist_data.to_hdf(data_path, key='float_volume')
        print("\nFinished updating data")


if __name__ == '__main__':
    begin = datetime.now()
    fv = FloatVolume()
    fv.rewrite_data()
    fv.update_data()
    end = datetime.now()
    print("\nTotal spend:", end - begin)
