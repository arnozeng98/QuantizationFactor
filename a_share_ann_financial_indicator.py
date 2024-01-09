__author__ = 'Lingsong Zeng'

from datetime import datetime
from bisect import bisect
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
from function.read_data_config import ReadConfig
from function.read_index_columns import ReadIndex


class AnnFinancialIndicator:
    """
    Read following data from wind.ASHAREANNFINANCIALINDICATOR and split them into 34 tables in following form

        S_INFO_WINDCODE: Wind代码  -> column
        YEAR -> index

         Factor                  | Comments
        -------------------------|--------------------------------------------------------
         S_FA_EPS_DILUTED        | 每股收益-摊薄 (元)
         S_FA_EPS_BASIC          | 每股收益-基本
         S_FA_EPS_DILUTED2       | 每股收益-稀释 (元)
         S_FA_EPS_EX             | 每股收益-扣除 (元)
         S_FA_EPS_EXBASIC        | 每股收益-扣除/基本
         S_FA_EPS_EXDILUTED      | 每股收益-扣除/稀释 (元)
         S_FA_BPS                | 每股净资产 (元)
         S_FA_BPS_SH             | 归属于母公司股东的每股净资产 (元)
         S_FA_BPS_ADJUST         | 每股净资产-调整(元)
         ROE_DILUTED             | 净资产收益率-摊薄 (%)
         ROE_WEIGHTED            | 净资产收益率-加权 (%)
         ROE_EX                  | 净资产收益率-扣除 (%)
         ROE_EXWEIGHTED          | 净资产收益率-扣除/加权 (%)
         NET_PROFIT              | 国际会计准则净利润(元)
         RD_EXPENSE              | 研发费用 (元)
         S_FA_EXTRAORDINARY      | 非经常性损益 (元)
         S_FA_CURRENT            | 流动比 (%)
         S_FA_QUICK              | 速动比 (%)
         S_FA_ARTURN             | 应手账款周转率 (%)
         S_FA_INVTURN            | 存货周转率 (%)
         S_FT_DEBTTOASSETS       | 资产负债率 (%)
         S_FA_OCFPS              | 每股经营活动产生的现金流量净额 (元)
         S_FA_YOYOCFPS           | 同比增长率.利润总额(%)
         S_FA_DEDUCTEDPROFIT     | 扣除非经常性损益后的净利润 (扣除少数股东损益) (%)
         S_FA_DEDUCTEDPROFIT_YOY | 同比增长率.扣除非经常性损益后的净利润 (扣除少数股东损益) (%)
         GROWTH_BPS_SH           | 比年初增长率.归属于母公司股东的每股净资产 (%)
         S_FA_YOYEQUITY          | 比年初增长率.归属母公司的股东权益 (%)
         YOY_ROE_DILUTED         | 同比增长率.净资产收益率 (摊薄) (%)
         YOY_NET_CASH_FLOWS      | 同比增长率.经营活动产生的现金流量净额 (%)
         S_FA_YOYEPS_BASIC       | 同比增长率.基本每股收益 (%)
         S_FA_YOYEPS_DILUTED     | 同比增长率.稀释每股收益 (%)
         S_FA_YOYOP              | 同比增长率.营业利润 (%)
         S_FA_YOYEBT             | 同比增长率.利润总额 (%)
         NET_PROFIT_YOY          | 同比增长率.净利润 (%)

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
        self.data_path = os.path.abspath(os.path.join(data_path, 'ASHAREANNFINANCIALINDICATOR'))
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

        self.target_column = ['S_FA_EPS_DILUTED', 'S_FA_EPS_BASIC', 'S_FA_EPS_DILUTED2', 'S_FA_EPS_EX',
                              'S_FA_EPS_EXBASIC', 'S_FA_EPS_EXDILUTED', 'S_FA_BPS', 'S_FA_BPS_SH', 'S_FA_BPS_ADJUST',
                              'ROE_DILUTED', 'ROE_WEIGHTED', 'ROE_EX', 'ROE_EXWEIGHTED', 'NET_PROFIT', 'RD_EXPENSE',
                              'S_FA_EXTRAORDINARY', 'S_FA_CURRENT', 'S_FA_QUICK', 'S_FA_ARTURN', 'S_FA_INVTURN',
                              'S_FT_DEBTTOASSETS', 'S_FA_OCFPS', 'S_FA_YOYOCFPS', 'S_FA_DEDUCTEDPROFIT',
                              'S_FA_DEDUCTEDPROFIT_YOY', 'GROWTH_BPS_SH', 'S_FA_YOYEQUITY', 'YOY_ROE_DILUTED',
                              'YOY_NET_CASH_FLOWS', 'S_FA_YOYEPS_BASIC', 'S_FA_YOYEPS_DILUTED', 'S_FA_YOYOP',
                              'S_FA_YOYEBT', 'NET_PROFIT_YOY']

        self.factor_name_str = ', '.join(self.target_column)  # transfer factor names to str for sql

    def read_data(self, begin_date: str = '20160101') -> pd.DataFrame:
        """
        Read data from wind.ASHAREANNFINANCIALINDICATOR
        :param begin_date: str (default '20160101')
        :return: DataFrame with ANN_DT, S_INFO_WINDCODE, self.target_column
        """

        begin = datetime.now()
        print("Reading data from server... ")

        sql = """
        SELECT S_INFO_WINDCODE, left(REPORT_PERIOD, 4) as 'YEAR', right(REPORT_PERIOD, 4) as 'SEASON', {}
        FROM wind.ASHAREANNFINANCIALINDICATOR
        WHERE REPORT_PERIOD >= {};
        """.format(self.factor_name_str, begin_date)

        data = pd.read_sql(sql, self.db)
        end = datetime.now()
        print("Finished reading data from server, spend:", end - begin)
        return data

    def read_exist_data(self, factor: str, i: int) -> object:
        """
        Read exist wind.ASHAREANNFINANCIALINDICATOR data of different factors from local
        :param factor: str
        :param i: int, season == i + 1
        :return: object, corresponding factor data stored in local
        """

        return pd.read_hdf(os.path.abspath(os.path.join(self.data_path, '{}_{}.hdf5'.format(factor, i+1))), key=factor)

    def rewrite_data(self):
        """Split data into 34 tables by factors"""

        financial_indicator = self.read_data()  # this will pull data after 20160101 as default
        financial_indicator.sort_values(['SEASON', 'S_INFO_WINDCODE', 'YEAR'], inplace=True)
        begin = datetime.now()

        main_dict = {s: {factor: {} for factor in self.target_column} for s in financial_indicator.SEASON.unique()}
        # season_list = [ {'S_FA_EPS_DILUTED': {}, 'S_FA_EPS_BASIC': {}, ...}  -> season_dict x4 ]

        data_iter = financial_indicator.itertuples()
        pre_record = next(data_iter)
        stock_dict = {factor: {} for factor in self.target_column}

        for record in data_iter:

            code = pre_record.S_INFO_WINDCODE
            year = pre_record.YEAR
            date = self.index[bisect(self.index, '{}0101'.format(year))]  # find the first date in this year

            for factor in self.target_column:
                value = getattr(pre_record, factor)
                if pd.isna(value):  # if value is NaN, we assign value as 'NA' to avoid be overwritten later
                    value = 'NA'
                stock_dict[factor][date] = value  # assign value to first date of the year

            if code != record.S_INFO_WINDCODE:
                for factor in self.target_column:
                    main_dict[pre_record.SEASON][factor][code] = stock_dict[factor].copy()
                    stock_dict[factor].clear()

            pre_record = record

        code = pre_record.S_INFO_WINDCODE
        year = pre_record.YEAR
        date = self.index[bisect(self.index, '{}0101'.format(year))]  # find the first date in this year

        for factor in self.target_column:
            value = getattr(pre_record, factor)
            if pd.isna(value):  # if value is NaN, we assign value as 'NA' to avoid be overwritten later
                value = 'NA'
            stock_dict[factor][date] = value  # assign value to first date of the year
            main_dict[pre_record.SEASON][factor][code] = stock_dict[factor].copy()

        for i, s in enumerate(financial_indicator.SEASON.unique()):  # ['0331', '0630', '0930', '1231']
            for factor in tqdm(self.target_column):
                df = pd.DataFrame(main_dict[s][factor], index=self.index, columns=self.column).fillna(method='ffill')
                df.replace('NA', np.nan, inplace=True)  # replace 'NA' by NaN
                df.to_hdf(os.path.abspath(os.path.join(self.data_path, '{}_{}.hdf5'.format(factor, i+1))), key=factor)

        end = datetime.now()
        print("Finished rewriting data, spend:", end - begin)

    def update_data(self):
        """
        Update local wind.ASHAREANNFINANCIALINDICATOR data of different factors generated before
        Note:
            1. if there is no new data to update, we end the function directly
            2. we start reading from the first date of new data, so we do not pull superfluous data from database
        """

        s_fa_eps_diluted = self.read_exist_data('S_FA_EPS_DILUTED', 0)  # pick one to compare if update is needed
        new_date = sorted(list(set(self.index) - set(s_fa_eps_diluted.index)))

        if len(new_date) == 0:  # new_date == [] means there is no new data to update, so we end the function
            print("Data is already updated!")
            return None  # function ends here

        financial_indicator = self.read_data(new_date[0])  # start reading from the first date in new_date
        financial_indicator.sort_values(['S_INFO_WINDCODE', 'YEAR'], inplace=True)
        begin = datetime.now()

        main_dict = {s: {factor: {} for factor in self.target_column} for s in financial_indicator.SEASON.unique()}
        # season_list = [ {'S_FA_EPS_DILUTED': {}, 'S_FA_EPS_BASIC': {}, ...}  -> season_dict x4 ]

        data_iter = financial_indicator.itertuples()
        pre_record = next(data_iter)
        stock_dict = {factor: {} for factor in self.target_column}

        for record in data_iter:

            code = pre_record.S_INFO_WINDCODE
            year = pre_record.YEAR
            date = self.index[bisect(self.index, '{}0101'.format(year))]  # find the first date in this year

            for factor in self.target_column:
                value = getattr(pre_record, factor)
                if pd.isna(value):  # if value is NaN, we assign value as 'NA' to avoid be overwritten later
                    value = 'NA'
                stock_dict[factor][date] = value  # assign value to first date of the year

            if code != record.S_INFO_WINDCODE:
                for factor in self.target_column:
                    main_dict[pre_record.SEASON][factor][code] = stock_dict[factor].copy()
                    stock_dict[factor].clear()

            pre_record = record

        code = pre_record.S_INFO_WINDCODE
        year = pre_record.YEAR
        date = self.index[bisect(self.index, '{}0101'.format(year))]  # find the first date in this year

        for factor in self.target_column:
            value = getattr(pre_record, factor)
            if pd.isna(value):  # if value is NaN, we assign value as 'NA' to avoid be overwritten later
                value = 'NA'
            stock_dict[factor][date] = value  # assign value to first date of the year
            main_dict[pre_record.SEASON][factor][code] = stock_dict[factor].copy()

        for i, s in enumerate(financial_indicator.SEASON.unique()):
            for factor in self.target_column:
                exist_data = self.read_exist_data(factor, i)
                df = pd.DataFrame(main_dict[s][factor], index=new_date, columns=self.column).fillna(method='ffill')
                df.replace('NA', np.nan, inplace=True)  # replace 'NA' by NaN
                df = exist_data.append(df)
                df.to_hdf(os.path.abspath(os.path.join(self.data_path, '{}_{}.hdf5'.format(factor, i+1))), key=factor)

        end = datetime.now()
        print("Finished updating data, spend:", end - begin)


if __name__ == '__main__':
    total_begin = datetime.now()
    afi = AnnFinancialIndicator()
    afi.rewrite_data()
    afi.update_data()
    total_end = datetime.now()
    print("\nTotal spend:", total_end - total_begin)
