__author__ = 'Lingsong Zeng'

from datetime import datetime
from tqdm import tqdm
import pandas as pd
import os
from function.read_data_config import ReadConfig
from function.read_index_columns import ReadIndex


class CertainScoreStk:
    """
    Read following data from suntime_ys_ce.certainty_score_stk and split them into 9 tables in following form

        stock_code: 股票代码  -> index
        con_date: 交易日期  -> column

         Factors         | Comments
        -----------------|-------------------
         score           | 个股评分
         profit_score    | 业绩总分
         value_score     | 估值总分
         market_score    | 个股评分变化率-1周
         score_grate_1w  | 个股评分变化率-4周
         score_grate_4w  | 个股评分变化率-4周
         score_grate_13w | 个股评分变化率-13周
         score_grate_26w | 个股评分变化率-26周
         score_grate_52w | 个股评分变化率-52周

                       | 600373.SH | 300557.SZ | 000416.SZ |  .......
             ----------|-----------|-----------|-----------|-----------
              20160104 |   49.75   |    nan    |   28.15   |
              20160105 |   49.75   |    nan    |   28.15   |  .......    -> x9
              20160106 |   49.75   |    nan    |   28.15   |
                 ...   |    ...    |    ...    |    ...    |
    """

    def __init__(self):
        """Initialize the class"""

        ri = ReadIndex()
        self.index = ri.read_index()     # For the date list we constructed before
        self.column = ri.read_columns()  # For the stock list we constructed before

        rc = ReadConfig()
        self.db = rc.read_suntime_mysql()

        data_path = rc.read_factor_path()  # for reading data from local and outputting data to local
        self.data_path = os.path.abspath(os.path.join(data_path, 'certainty_score_stk'))
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

        self.target_column = ['score', 'profit_score', 'value_score', 'market_score', 'score_grate_1w',
                              'score_grate_4w', 'score_grate_13w', 'score_grate_26w', 'score_grate_52w']

    def read_data(self, begin_date: str = '20160101') -> pd.DataFrame:
        """
        Read data from suntime_ys_ce.certainty_score_stk
        :param begin_date: str (default '20160101')
        :return: DataFrame with con_date, stock_code, self.target_column

                     | con_date | stock_code | ... | score_grate_26w | score_grate_52w
            ---------|----------|------------|-----|-----------------|-----------------
                0    | 20170920 | 000037.SZ  | ... |     -0.0574     |      0.1543
                1    | 20170920 | 000043.SZ  | ... |      0.0695     |     -0.0212
                2    | 20170920 | 000100.SZ  | ... |     -0.2458     |      0.1188
               ...   |   ...    |    ...     | ... |       ...       |       ...
             4088623 | 20201124 | 688788.SH  | ... |       NaN       |       NaN
             4088624 | 20201124 | 688981.SH  | ... |       NaN       |       NaN
             4088625 | 20201124 | 689009.SH  | ... |       NaN       |       NaN  -> np.NaN
        """

        begin = datetime.now()
        print("Reading data from server... ")

        sql = """
        SELECT DATE_FORMAT(con_date, '%Y%m%d') AS con_date,
               IF(stock_code REGEXP '^6', CONCAT(stock_code, '.SH'), CONCAT(stock_code, '.SZ')) AS stock_code,
               {}, {}, {}, {}, {}, {}, {}, {}, {}
        FROM suntime.certainty_score_stk
        WHERE index_code = 999999 AND con_date >= {};
        """.format(*self.target_column, begin_date)

        data = pd.read_sql(sql, self.db)
        end = datetime.now()
        print("Finished reading data from server, spend:", end - begin)
        return data

    def read_exist_data(self, factor: str) -> object:
        """
        Read exist certainty_score_stk data of different factors from local
        :param factor: str
        :return: object, corresponding factor data stored in local
        """

        return pd.read_hdf(os.path.abspath(os.path.join(self.data_path, '{}.hdf5'.format(factor))), key=factor)

    def rewrite_data(self):
        """Split data into 9 tables by factors"""

        certainty_score_stk = self.read_data()  # this will pull data after 20160101 as default

        begin = datetime.now()
        unstack_data = certainty_score_stk.groupby(['con_date', 'stock_code']).max().unstack()

        # ==============================================================================================================
        # unstack_data:
        #                       score                      ... score_grate_52w                      -> column 1
        #    ->  stock_code 000001.SZ 000002.SZ 000004.SZ  ...       688788.SH 688981.SH 689009.SH  -> column 2
        #    ->  con_date                                  ...
        #        20160104       56.35     75.95     52.55  ...             NaN       NaN       NaN  -> np.NaN
        #        20160105       56.35     75.95     52.55  ...             NaN       NaN       NaN
        #        20160106       56.35     75.95     52.55  ...             NaN       NaN       NaN
        #        ...              ...       ...       ...  ...             ...       ...       ...
        #        20201120       64.15     73.90     48.45  ...             NaN       NaN       NaN
        #        20201123       64.15     78.90     48.45  ...             NaN       NaN       NaN
        #        20201124       64.15     83.90     48.45  ...             NaN       NaN       NaN
        # ==============================================================================================================

        # output to hdf5 file for corresponding factor in self.target_column
        for factor in tqdm(self.target_column):
            factor_table = unstack_data[factor].reindex(columns=self.column)
            factor_table.to_hdf(os.path.abspath(os.path.join(self.data_path, factor + '.hdf5')), key=factor)

        end = datetime.now()
        print("Finished rewriting data, spend:", end - begin)

    def update_data(self):
        """
        Update local certainty_score_stk data of different factors generated before
        Note:
            1. if there is no new data to update, we end the function directly
            2. we start reading from the first date of new data, so we do not pull superfluous data from database
        """

        score = self.read_exist_data('score')  # pick one of table from local to compare if update is needed
        new_date = sorted(list(set(self.index) - set(score.index)))

        if len(new_date) == 0:  # new_date == [] means there is no new data to update, so we end the function
            print("Data is already updated!")
            return None  # function ends here

        certainty_score_stk = self.read_data(new_date[0])  # start reading from the first date in new_date

        begin = datetime.now()
        unstack_data = certainty_score_stk.groupby(['con_date', 'stock_code']).max().unstack()

        # append update data to old data (from local) for each factor
        for factor in tqdm(self.target_column):
            exist_data = self.read_exist_data(factor)
            factor_table = exist_data.append(unstack_data[factor]).reindex(columns=self.column)
            factor_table.to_hdf(os.path.abspath(os.path.join(self.data_path, factor + '.hdf5')), key=factor)

        end = datetime.now()
        print("Finished updating data, spend:", end - begin)


if __name__ == '__main__':
    total_begin = datetime.now()
    css = CertainScoreStk()
    css.rewrite_data()
    css.update_data()
    total_end = datetime.now()
    print("\nTotal spend:", total_end - total_begin)
