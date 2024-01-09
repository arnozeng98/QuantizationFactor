__author__ = 'Lingsong Zeng'

from datetime import datetime
from tqdm import tqdm
import pandas as pd
import os
from function.read_data_config import ReadConfig
from function.read_index_columns import ReadIndex


class L2Indicators:
    """
    Read following data from wind.ASHAREL2INDICATORS and split them into 12 tables in following form

        S_INFO_WINDCODE: Wind代码  -> column
        TRADE_DT: 交易日期  -> index

         Factor                   | Comments
        --------------------------|----------------
        S_LI_INITIATIVEBUYRATE    | 主买比率 (%)
        S_LI_INITIATIVEBUYMONEY   | 主买总额 (万元)
        S_LI_INITIATIVEBUYAMOUNT  | 主买总量 (手)
        S_LI_INITIATIVESELLRATE   | 主卖比率 (%)
        S_LI_INITIATIVESELLMONEY  | 主卖总额 (万元)
        S_LI_INITIATIVESELLAMOUNT | 主卖总量 (手)：
        S_LI_LARGEBUYRATE         | 大买比率 (%)
        S_LI_LARGEBUYMONEY        | 大买总量 (万元)
        S_LI_LARGEBUYAMOUNT       | 大买总量 (手)
        S_LI_LARGESELLRATE        | 大卖比率 (%)
        S_LI_LARGESELLMONEY       | 大卖总额 (万元)
        S_LI_LARGESELLAMOUNT      | 大卖总量 (手)
        S_LI_ENTRUSTRATE          | 总委比 (%)
        S_LI_ENTRUDIFFERAMOUNT    | 总委差量 (手)
        S_LI_ENTRUDIFFERAMONEY    | 总委差额 (万元)
        S_LI_ENTRUSTBUYMONEY      | 总委买额 (万元)
        S_LI_ENTRUSTSELLMONEY     | 总委卖额 (万元)
        S_LI_ENTRUSTBUYAMOUNT     | 总委买量 (手)
        S_LI_ENTRUSTSELLAMOUNT    | 总委卖量 (手)

                      | 600373.SH | 300557.SZ | ... | 002011.SZ | 600129.SH
            ----------|-----------|-----------|-----|-----------|-----------
             20160104 |  0.2198   |    NaN    | ... |    NaN    |  0.2287
             20160105 |  0.3436   |    NaN    | ... |    NaN    |  0.3544       -> x12
             20160106 |  0.3434   |    NaN    | ... |    NaN    |  0.3565
                ...   |     ...   |    ...    | ... |    ...    |     ...
    """

    def __init__(self):
        """Initialize the class"""

        ri = ReadIndex()
        self.index = ri.read_index()     # For the date list we constructed before
        self.column = ri.read_columns()  # For the stock list we constructed before

        rc = ReadConfig()
        self.db = rc.read_wind_mysql()

        data_path = rc.read_factor_path()  # for reading data from local and outputting data to local
        self.data_path = os.path.abspath(os.path.join(data_path, 'ASHAREL2INDICATORS'))
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

        self.target_column = ['S_LI_INITIATIVEBUYRATE', 'S_LI_INITIATIVEBUYMONEY', 'S_LI_INITIATIVEBUYAMOUNT',
                              'S_LI_INITIATIVESELLRATE', 'S_LI_INITIATIVESELLMONEY', 'S_LI_INITIATIVESELLAMOUNT',
                              'S_LI_LARGEBUYRATE', 'S_LI_LARGEBUYMONEY', 'S_LI_LARGEBUYAMOUNT', 'S_LI_LARGESELLRATE',
                              'S_LI_LARGESELLMONEY', 'S_LI_LARGESELLAMOUNT', 'S_LI_ENTRUSTRATE',
                              'S_LI_ENTRUDIFFERAMOUNT', 'S_LI_ENTRUDIFFERAMONEY', 'S_LI_ENTRUSTBUYMONEY',
                              'S_LI_ENTRUSTSELLMONEY', 'S_LI_ENTRUSTBUYAMOUNT', 'S_LI_ENTRUSTSELLAMOUNT']

        self.factor_name_str = ', '.join(self.target_column)  # transfer factor names to str for sql

    def read_data(self, begin_date: str = '20160101') -> pd.DataFrame:
        """
        Read data from wind.ASHAREL2INDICATORS
        :param begin_date: str (default '20160101')
        :return: DataFrame with TRADE_DT, S_INFO_WINDCODE, self.target_column
        """

        begin = datetime.now()
        print("Reading data from server, this process would normally spend around 3 minutes... ")

        sql = """
        SELECT S_INFO_WINDCODE, TRADE_DT, {}
        FROM wind.ASHAREL2INDICATORS
        WHERE TRADE_DT >= {};
        """.format(self.factor_name_str, begin_date)

        data = pd.read_sql(sql, self.db)
        end = datetime.now()
        print("Finished reading data from server, spend:", end - begin)
        return data

    def read_exist_data(self, factor: str) -> object:
        """
        Read exist wind.ASHAREL2INDICATORS data of different factors from local
        :param factor: str
        :return: object, corresponding factor data stored in local
        """

        return pd.read_hdf(os.path.abspath(os.path.join(self.data_path, '{}.hdf5'.format(factor))), key=factor)

    def rewrite_data(self):
        """Split data into 12 tables by factors"""

        a_share_l2indicators = self.read_data()  # this will pull data after 20160101 as default
        begin = datetime.now()
        unstack_data = a_share_l2indicators.groupby(['TRADE_DT', 'S_INFO_WINDCODE']).max().unstack()

        # output to hdf5 file for corresponding factor in self.target_column
        for factor in tqdm(self.target_column):
            factor_table = unstack_data[factor].reindex(columns=self.column)
            factor_table.to_hdf(os.path.abspath(os.path.join(self.data_path, factor + '.hdf5')), key=factor)

        end = datetime.now()
        print("Finished rewriting data, spend:", end - begin)

    def update_data(self):
        """
        Update local wind.ASHAREL2INDICATORS data of different factors generated before
        Note:
            1. if there is no new data to update, we end the function directly
            2. we start reading from the first date of new data, so we do not pull superfluous data from database
        """

        entrustrate = self.read_exist_data('S_LI_ENTRUSTRATE')  # pick one to compare if update is needed
        new_date = sorted(list(set(self.index) - set(entrustrate.index)))

        if len(new_date) == 0:  # new_date == [] means there is no new data to update, so we end the function
            print("Data is already updated!")
            return None  # function ends here

        a_share_income = self.read_data(new_date[0])  # start reading from the first date in new_date
        begin = datetime.now()
        unstack_data = a_share_income.groupby(['TRADE_DT', 'S_INFO_WINDCODE']).max().unstack()

        # append update data to old data (from local) for each factor
        for factor in tqdm(self.target_column):
            exist_data = self.read_exist_data(factor)
            factor_table = exist_data.append(unstack_data[factor]).reindex(columns=self.column)
            factor_table.to_hdf(os.path.abspath(os.path.join(self.data_path, factor + '.hdf5')), key=factor)

        end = datetime.now()
        print("Finished updating data, spend:", end - begin)


if __name__ == '__main__':
    total_begin = datetime.now()
    l2 = L2Indicators()
    l2.rewrite_data()
    l2.update_data()
    total_end = datetime.now()
    print("\nTotal spend:", total_end - total_begin)
