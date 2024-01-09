__author__ = 'Lingsong Zeng'

from datetime import datetime
import pandas as pd
import os
from function.read_data_config import ReadConfig
from function.read_index_columns import ReadIndex


class HolderNumber:

    def __init__(self):
        """Initialize the class"""

        # initialize parameters we gonna use later
        ri = ReadIndex()
        self.index = ri.read_index()  # For the date list we constructed before
        self.column = ri.read_columns()  # For the stock list we constructed before

        rc = ReadConfig()
        self.db = rc.read_wind_mysql()

        data_path = rc.read_factor_path()  # for reading data from local and outputting data to local
        self.data_path = os.path.abspath(os.path.join(data_path, 'ASHAREHOLDERNUMBER'))
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

    def read_data_before_begin_date(self, begin_date: str) -> pd.DataFrame:
        """
        Read data one date before the begin_date
        :param begin_date: str
        :return: pd.DataFrame
        """

        sql = """
            SELECT f.S_INFO_WINDCODE, f.ANN_DT, f.S_HOLDER_ENDDATE, f.S_HOLDER_NUM
            FROM (
                SELECT S_INFO_WINDCODE, max(ANN_DT) as LAST_ANN_DT
                FROM wind.ASHAREHOLDERNUMBER
                WHERE ANN_DT < {} AND S_INFO_WINDCODE NOT REGEXP '^[a-zA-Z]'
                GROUP BY S_INFO_WINDCODE
            ) AS x INNER JOIN wind.ASHAREHOLDERNUMBER AS f ON f.S_INFO_WINDCODE = x.S_INFO_WINDCODE
            AND f.ANN_DT = x.LAST_ANN_DT
            """.format(begin_date)

        return pd.read_sql(sql, self.db)

    def read_data_after_begin_date(self, begin_date: str) -> pd.DataFrame:
        """
        Read data from begin_date
        :param begin_date: str
        :return: pd.DataFrame
        """

        sql = """
            SELECT S_INFO_WINDCODE, ANN_DT, S_HOLDER_ENDDATE, S_HOLDER_NUM
            FROM wind.ASHAREHOLDERNUMBER
            WHERE ANN_DT >= {} AND S_INFO_WINDCODE NOT REGEXP '^[a-zA-Z]';
            """.format(begin_date)

        return pd.read_sql(sql, self.db)

    def read_data(self, begin_date: str = '20160101') -> pd.DataFrame:
        """
        Read wind.ASHAREHOLDERNUMBER data from database
        :param begin_date: str  (default '20160101')
        :return: DataFrame
        """
        begin = datetime.now()
        print("Reading data from server... ")

        data_a = self.read_data_before_begin_date(begin_date)
        data_b = self.read_data_after_begin_date(begin_date)

        end = datetime.now()
        print("Finished reading data from server, spend:", end - begin)
        return data_a.append(data_b, ignore_index=True)

    def read_exist_data(self) -> object:
        """
        Read exist wind.ASHAREHOLDERNUMBER data from local
        :return: object
        """

        data_path = os.path.abspath(os.path.join(self.data_path, 'holder_number.hdf5'))
        return pd.read_hdf(data_path, key='holder_number')

    def rewrite_data(self):
        a_share_holder_number = self.read_data()  # begin_date will set to '20160101' as default
        a_share_holder_number.sort_values(['S_INFO_WINDCODE', 'ANN_DT', 'S_HOLDER_ENDDATE'], inplace=True)

        data_iter = a_share_holder_number.itertuples()
        pre_record = next(data_iter)
        holder_number = {}
        stock_dict = {}

        for record in data_iter:
            _, code_1, ann_dt_1, end_dt_1, holder_num_1 = pre_record
            _, code_2, ann_dt_2, end_dt_2, holder_num_2 = record

            if code_1 != code_2:
                stock_dict[ann_dt_1] = holder_num_1
                holder_number[code_1] = stock_dict.copy()
                stock_dict.clear()
            else:
                if ann_dt_1 == ann_dt_2:
                    pass
                else:
                    stock_dict[ann_dt_1] = holder_num_1
            pre_record = record

        _, code_1, ann_dt_1, end_dt_1, holder_num_1 = pre_record
        stock_dict[ann_dt_1] = holder_num_1
        holder_number[code_1] = stock_dict.copy()

        index_range = sorted(list(set(self.index) | set(a_share_holder_number.ANN_DT)))
        df = pd.DataFrame(holder_number, index=index_range).fillna(method='ffill')
        df = df.reindex(index=self.index, columns=self.column)
        df.to_hdf(os.path.abspath(os.path.join(self.data_path, 'holder_number.hdf5')), key='holder_number')

    def update_data(self):
        """
        Update local data generated before
        Note:
            1. if there is no new data to update, we end the function directly
            2. we start reading from the first date of new data, so we do not pull superfluous data from database
        """

        # exist_data from local
        holder_number_local = self.read_exist_data()
        new_date = sorted(list(set(self.index) - set(holder_number_local.index)))

        # len(new_date) == 0 means no need to update so we end the function
        if len(new_date) == 0:
            print("\nData is already updated.")
            return None  # function ends here

        a_share_holder_number = self.read_data(new_date[0])
        a_share_holder_number.sort_values(['S_INFO_WINDCODE', 'ANN_DT', 'S_HOLDER_ENDDATE'], inplace=True)

        data_iter = a_share_holder_number.itertuples()
        pre_record = next(data_iter)
        holder_number = {}
        stock_dict = {}

        for record in data_iter:
            _, code_1, ann_dt_1, end_dt_1, holder_num_1 = pre_record
            _, code_2, ann_dt_2, end_dt_2, holder_num_2 = record

            if code_1 != code_2:
                stock_dict[ann_dt_1] = holder_num_1
                holder_number[code_1] = stock_dict.copy()
                stock_dict.clear()
            else:
                if ann_dt_1 == ann_dt_2:
                    pass
                else:
                    stock_dict[ann_dt_1] = holder_num_1
            pre_record = record

        _, code_1, ann_dt_1, end_dt_1, holder_num_1 = pre_record
        stock_dict[ann_dt_1] = holder_num_1
        holder_number[code_1] = stock_dict.copy()

        index_range = sorted(list(set(self.index) | set(a_share_holder_number.ANN_DT)))
        df = pd.DataFrame(holder_number, index=index_range).fillna(method='ffill')
        df = holder_number_local.append(df).reindex(index=self.index, columns=self.column)
        df.to_hdf(os.path.abspath(os.path.join(self.data_path, 'holder_number.hdf5')), key='holder_number')


if __name__ == '__main__':
    total_begin = datetime.now()
    hn = HolderNumber()
    hn.rewrite_data()
    hn.update_data()
    total_end = datetime.now()
    print("\nTotal spend:", total_end - total_begin)
