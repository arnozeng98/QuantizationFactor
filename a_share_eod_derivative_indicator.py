__author__ = 'Lingsong Zeng'

from datetime import datetime
from tqdm import tqdm
import pandas as pd
import os
from function.read_data_config import ReadConfig
from function.read_index_columns import ReadIndex


class EodDerivativeIndicator:
    """
    Read following data from wind.ASHAREEODDERIVATIVEINDICATOR and split them into 33 tables in following form

        S_INFO_WINDCODE: Wind代码  -> column
        TRADE_DT: 公告日期  -> index

         Factor                      | Comments
        -----------------------------|------------------------------
         S_VAL_MV                    | 当日总市值
         S_DQ_MV                     | 当日流通市值
         S_PQ_HIGH_52W_              | 52周最高价
         S_PQ_LOW_52W_               | 52周最低价
         S_VAL_PE                    | 市盈率 (PE)
         S_VAL_PB_NEW                | 市净率 (PB)
         S_VAL_PE_TTM                | 市盈率 (PE, TTM)
         S_VAL_PCF_OCF               | 市现率 (PCF, 经营现金流)
         S_VAL_PCF_OCFTTM            | 市现率 (PCF, 经营现金流TTM)
         S_VAL_PCF_NCF               | 市现率 (PCF, 现金净流量)
         S_VAL_PCF_NCFTTM            | 市现率 (PCF, 现金净流量TTM)
         S_VAL_PS                    | 市销率 (PS)
         S_VAL_PS_TTM                | 市销率 (PS, TTM)
         S_DQ_TURN                   | 换手率
         S_DQ_FREETURNOVER           | 换手率 (基准.自由流通股本)
         TOT_SHR_TODAY               | 当日总股本
         FLOAT_A_SHR_TODAY           | 当日流通股本
         S_DQ_CLOSE_TODAY            | 当日收盘价
         S_PRICE_DIV_DPS             | 股价/每股派息
         S_PQ_ADJHIGH_52W            | 52周最高价 (复权)
         S_PQ_ADJLOW_52W             | 52周最低价 (复权)
         FREE_SHARES_TODAY           | 当日自由流通股本
         NET_PROFIT_PARENT_COMP_TTM  | 归属母公司净利润 (TTM)
         NET_PROFIT_PARENT_COMP_LYR  | 归属母公司净利润 (LYR)
         NET_ASSETS_TODAY            | 当日净资产
         NET_CASH_FLOWS_OPER_ACT_TTM | 经营活动产生的现金流量净额 (TTM)
         NET_CASH_FLOWS_OPER_ACT_LYR | 经营活动产生的现金流量净额 (LYR)
         OPER_REV_TTM                | 营业收入 (TTM)
         OPER_REV_LYR                | 营业收入 (LYR)
         NET_INCR_CASH_CASH_EQU_TTM  | 现金及现金等价物净增加额 (TTM)
         NET_INCR_CASH_CASH_EQU_LYR  | 现金及现金等价物净增加额 (LYR)
         UP_DOWN_LIMIT_STATUS        | 涨跌停状态
         LOWEST_HIGHEST_STATUS       | 最高最低价状态

    """

    def __init__(self):
        """Initialize the class"""

        # initialize parameters we gonna use later
        ri = ReadIndex()
        self.index = ri.read_index()     # For the date list we constructed before
        self.column = ri.read_columns()  # For the stock list we constructed before

        rc = ReadConfig()
        self.db = rc.read_wind_mysql()

        data_path = rc.read_factor_path()  # for reading data from local and outputting data to local
        self.data_path = os.path.abspath(os.path.join(data_path, 'ASHAREEODDERIVATIVEINDICATOR'))
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

        self.target_column = ['S_VAL_MV', 'S_DQ_MV', 'S_PQ_HIGH_52W_', 'S_PQ_LOW_52W_', 'S_VAL_PE', 'S_VAL_PB_NEW',
                              'S_VAL_PE_TTM', 'S_VAL_PCF_OCF', 'S_VAL_PCF_OCFTTM', 'S_VAL_PCF_NCF', 'S_VAL_PCF_NCFTTM',
                              'S_VAL_PS', 'S_VAL_PS_TTM', 'S_DQ_TURN', 'S_DQ_FREETURNOVER', 'TOT_SHR_TODAY',
                              'FLOAT_A_SHR_TODAY', 'S_DQ_CLOSE_TODAY', 'S_PRICE_DIV_DPS', 'S_PQ_ADJHIGH_52W',
                              'S_PQ_ADJLOW_52W', 'FREE_SHARES_TODAY', 'NET_PROFIT_PARENT_COMP_TTM',
                              'NET_PROFIT_PARENT_COMP_LYR', 'NET_ASSETS_TODAY', 'NET_CASH_FLOWS_OPER_ACT_TTM',
                              'NET_CASH_FLOWS_OPER_ACT_LYR', 'OPER_REV_TTM', 'OPER_REV_LYR',
                              'NET_INCR_CASH_CASH_EQU_TTM', 'NET_INCR_CASH_CASH_EQU_LYR', 'UP_DOWN_LIMIT_STATUS',
                              'LOWEST_HIGHEST_STATUS']

        self.factor_name_str = ', '.join(self.target_column)  # transfer factor names to str for sql

    def read_data(self, begin_date: str = '20160101') -> pd.DataFrame:
        """
        Read data from wind.ASHAREEODDERIVATIVEINDICATOR
        :param begin_date: str (default '20160101')
        :return: DataFrame with TRADE_DT, S_INFO_WINDCODE, self.target_column
        """

        begin = datetime.now()
        print("Reading data from server, this process normally takes around 9 minutes... ")

        chunk_size = 1000000
        offset = 0
        dfs = []
        while True:
            sql = """
            SELECT S_INFO_WINDCODE, TRADE_DT, {}
            FROM wind.ASHAREEODDERIVATIVEINDICATOR
            WHERE TRADE_DT >= {}
            LIMIT {}
            OFFSET {};
            """.format(self.factor_name_str, begin_date, chunk_size, offset)
            dfs.append(pd.read_sql(sql, self.db))
            offset += chunk_size
            if len(dfs[-1]) < chunk_size:
                break
        data = pd.concat(dfs)
        end = datetime.now()
        print("Finished reading data from server, spend:", end - begin)
        return data

    def read_exist_data(self, factor: str) -> object:
        """
        Read exist wind.ASHAREEODDERIVATIVEINDICATOR data of different factors from local
        :param factor: str
        :return: object, corresponding factor data stored in local
        """

        return pd.read_hdf(os.path.abspath(os.path.join(self.data_path, '{}.hdf5'.format(factor))), key=factor)

    def rewrite_data(self):
        """Split data into 33 tables by factors"""

        eod_derivative_indicator = self.read_data()  # this will pull data after 20160101 as default
        begin = datetime.now()
        unstack_data = eod_derivative_indicator.groupby(['TRADE_DT', 'S_INFO_WINDCODE']).max().unstack()

        # output to hdf5 file for corresponding factor in self.target_column
        for factor in tqdm(self.target_column):
            factor_table = unstack_data[factor].reindex(columns=self.column)
            factor_table.to_hdf(os.path.abspath(os.path.join(self.data_path, '{}.hdf5'.format(factor))), key=factor)

        end = datetime.now()
        print("Finished rewriting data, spend:", end - begin)

    def update_data(self):
        """
        Update local wind.ASHAREEODDERIVATIVEINDICATOR data of different factors generated before
        Note:
            1. if there is no new data to update, we end the function directly
            2. we start reading from the first date of new data, so we do not pull superfluous data from database
        """

        s_val_mv = self.read_exist_data('S_VAL_MV')  # pick one to compare if update is needed
        new_date = sorted(list(set(self.index) - set(s_val_mv.index)))

        if len(new_date) == 0:  # new_date == [] means there is no new data to update, so we end the function
            print("Data is already updated!")
            return None  # function ends here

        eod_derivative_indicator = self.read_data(new_date[0])  # start reading from the first date in new_date
        begin = datetime.now()
        unstack_data = eod_derivative_indicator.groupby(['TRADE_DT', 'S_INFO_WINDCODE']).max().unstack()

        # append update data to old data (from local) for each factor
        for factor in tqdm(self.target_column):
            exist_data = self.read_exist_data(factor)
            factor_table = exist_data.append(unstack_data[factor]).reindex(columns=self.column)
            factor_table.to_hdf(os.path.abspath(os.path.join(self.data_path, '{}.hdf5'.format(factor))), key=factor)

        end = datetime.now()
        print("Finished updating data, spend:", end - begin)


if __name__ == '__main__':
    total_begin = datetime.now()
    edi = EodDerivativeIndicator()
    edi.rewrite_data()
    edi.update_data()
    total_end = datetime.now()
    print("\nTotal spend:", total_end - total_begin)
