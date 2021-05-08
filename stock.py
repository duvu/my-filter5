import pandas as pd
import pymysql
import talib

from technical.consensus.summary import SummaryConsensus


class Stock:
    df_minute = None
    df_day = None
    df_finance = None

    def __init__(self, code, resolution='D', update_last=False, length=365):
        self.code = code
        self.resolution = resolution
        self.conn = pymysql.connect(host='localhost', user='admin', password='123456', database='mystocks')

        # Load finance info
        self.__load_finance_info()
        # Load price board at $D
        self.__load_price_board_day()
        # Load price board at $M
        self.__load_price_board_minute()

        if not self.df_finance.empty:
            # finance info #EPS means()
            self.EPS = self.df_finance['eps'].iloc[0]
            self.EPS_MEAN4 = self.df_finance['eps'].mean()
            rev_df_fi = self.df_finance['eps'][::-1]
            self.df_finance['eps_changed'] = rev_df_fi.pct_change()

            self.BVPS = self.df_finance['bvps'].iloc[0]
            self.BVPS_MEAN4 = self.df_finance['bvps'].mean()
            self.PE = self.df_finance['pe'].iloc[0]
            self.PE_MEAN4 = self.df_finance['pe'].mean()
            self.ROS = self.df_finance['ros'].iloc[0]
            self.ROS_MEAN4 = self.df_finance['ros'].mean()
            self.ROEA = self.df_finance['roea'].iloc[0]
            self.ROEA_MEAN4 = self.df_finance['roea'].mean()
            self.ROAA = self.df_finance['roaa'].iloc[0]
            self.ROAA_MEAN4 = self.df_finance['roaa'].mean()
            self.CURRENT_ASSETS = self.df_finance['current_assets'].iloc[0]
            self.CURRENT_ASSETS_MEAN4 = self.df_finance['current_assets'].mean()
            self.TOTAL_ASSETS = self.df_finance['total_assets'].iloc[0]
            self.TOTAL_ASSETS_MEAN4 = self.df_finance['total_assets'].mean()
            self.LIABILITIES = self.df_finance['liabilities'].iloc[0]
            self.LIABILITIES_MEAN4 = self.df_finance['liabilities'].mean()
            self.SHORT_LIABILITIES = self.df_finance['short_term_liabilities'].iloc[0]
            self.SHORT_LIABILITIES_MEAN4 = self.df_finance['short_term_liabilities'].mean()
            self.OWNER_EQUITY = self.df_finance['owner_equity'].iloc[0]
            self.OWNER_EQUITY_MEAN4 = self.df_finance['owner_equity'].mean()
            self.MINORITY_INTEREST = self.df_finance['minority_interest'].iloc[0]
            self.MINORITY_INTEREST_MEAN4 = self.df_finance['minority_interest'].mean()
            self.NET_REVENUE = self.df_finance['net_revenue'].iloc[0]
            self.NET_REVENUE_MEAN4 = self.df_finance['net_revenue'].mean()
            self.GROSS_PROFIT = self.df_finance['gross_profit'].iloc[0]
            self.GROSS_PROFIT_MEAN4 = self.df_finance['gross_profit'].mean()
            self.OPERATING_PROFIT = self.df_finance['operating_profit'].iloc[0]
            self.OPERATING_PROFIT_MEAN4 = self.df_finance['operating_profit'].mean()
            self.PROFIT_AFTER_TAX = self.df_finance['profit_after_tax'].iloc[0]
            self.PROFIT_AFTER_TAX_MEAN4 = self.df_finance['profit_after_tax'].mean()
            self.NET_PROFIT = self.df_finance['net_profit'].iloc[0]
            self.NET_PROFIT_MEAN4 = self.df_finance['net_profit'].mean()
        else:
            # finance info #EPS means()
            self.EPS = 0
            self.EPS_MEAN4 = 0
            rev_df_fi = self.df_finance['eps'][::-1]
            self.df_finance['eps_changed'] = rev_df_fi.pct_change()

            # print(self.df_fi['eps'])
            # print(self.df_fi['eps_changed'].mean())

            self.BVPS = 0
            self.BVPS_MEAN4 = 0
            self.PE = 0
            self.PE_MEAN4 = 0
            self.ROS = 0
            self.ROS_MEAN4 = 0
            self.ROEA = 0
            self.ROEA_MEAN4 = 0
            self.ROAA = 0
            self.ROAA_MEAN4 = 0
            self.CURRENT_ASSETS = 0
            self.CURRENT_ASSETS_MEAN4 = 0
            self.TOTAL_ASSETS = 0
            self.TOTAL_ASSETS_MEAN4 = 0
            self.LIABILITIES = 0
            self.LIABILITIES_MEAN4 = 0
            self.SHORT_LIABILITIES = 0
            self.SHORT_LIABILITIES_MEAN4 = 0
            self.OWNER_EQUITY = 0
            self.OWNER_EQUITY_MEAN4 = 0
            self.MINORITY_INTEREST = 0
            self.MINORITY_INTEREST_MEAN4 = 0
            self.NET_REVENUE = 0
            self.NET_REVENUE_MEAN4 = 0
            self.GROSS_PROFIT = 0
            self.GROSS_PROFIT_MEAN4 = 0
            self.OPERATING_PROFIT = 0
            self.OPERATING_PROFIT_MEAN4 = 0
            self.PROFIT_AFTER_TAX = 0
            self.PROFIT_AFTER_TAX_MEAN4 = 0
            self.NET_PROFIT = 0
            self.NET_PROFIT_MEAN4 = 0

        # reverse
        self.df_day = self.df_day.reindex(index=self.df_day.index[::-1])
        self.df_minute = self.df_minute.reindex(index=self.df_minute.index[::-1])

        try:
            print('Size Days #', self.code, len(self.df_day))
            self.consensus_day = SummaryConsensus(self.df_day)
            # self.consensus_minute = SummaryConsensus(self.df_minute)
        except Exception as ex:
            print('Something went wrong', ex)
            raise ex

        # print('Price List', self.df)
        if not self.df_day.empty:
            self.LAST_SESSION = self.df_day['t'].iloc[-1]
            self.df_day['changed'] = self.df_day['close'].pct_change()
            self.df_day['rsi'] = talib.RSI(self.df_day['close'])
            self.df_day['cci'] = talib.CCI(self.df_day['high'], self.df_day['low'], self.df_day['close'], timeperiod=20)

            self.df_day['macd'], self.df_day['macdsignal'], self.df_day['macdhist'] = talib.MACD(self.df_day['close'], fastperiod=12,
                                                                                                 slowperiod=26, signalperiod=9)

            self.CURRENT_CLOSE = self.df_day['close'].iloc[-1]
            self.df_day['SMA_5'] = talib.SMA(self.df_day['close'], timeperiod=5)
            self.df_day['SMA_10'] = talib.SMA(self.df_day['close'], timeperiod=10)
            self.df_day['SMA_20'] = talib.SMA(self.df_day['close'], timeperiod=20)
            self.df_day['SMA_50'] = talib.SMA(self.df_day['close'], timeperiod=50)
            self.df_day['SMA_150'] = talib.SMA(self.df_day['close'], timeperiod=150)
            self.df_day['SMA_200'] = talib.SMA(self.df_day['close'], timeperiod=200)
            # Volume SMA 20
            self.df_day['V_SMA_20'] = talib.SMA(self.df_day['volume'], timeperiod=20)
            self.LAST_V_SMA_20 = self.df_day['V_SMA_20'].iloc[-1]

            self.CDL2CROWS = talib.CDL2CROWS(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDL3BLACKCROWS = talib.CDL3BLACKCROWS(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDL3INSIDE = talib.CDL3INSIDE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDL3LINESTRIKE = talib.CDL3LINESTRIKE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDL3OUTSIDE = talib.CDL3OUTSIDE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDL3STARSINSOUTH = talib.CDL3STARSINSOUTH(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDL3WHITESOLDIERS = talib.CDL3WHITESOLDIERS(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLABANDONEDBABY = talib.CDLABANDONEDBABY(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDLADVANCEBLOCK = talib.CDLADVANCEBLOCK(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLBELTHOLD = talib.CDLBELTHOLD(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLBREAKAWAY = talib.CDLBREAKAWAY(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLCLOSINGMARUBOZU = talib.CDLCLOSINGMARUBOZU(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLCONCEALBABYSWALL = talib.CDLCONCEALBABYSWALL(self.df_day['open'].values, self.df_day['high'].values,
                                                                 self.df_day['low'].values, self.df_day['close'].values)
            self.CDLCOUNTERATTACK = talib.CDLCOUNTERATTACK(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDLDARKCLOUDCOVER = talib.CDLDARKCLOUDCOVER(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLDOJI = talib.CDLDOJI(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                         self.df_day['close'].values)
            self.CDLDOJISTAR = talib.CDLDOJISTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLDRAGONFLYDOJI = talib.CDLDRAGONFLYDOJI(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDLENGULFING = talib.CDLENGULFING(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLEVENINGDOJISTAR = talib.CDLEVENINGDOJISTAR(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLEVENINGSTAR = talib.CDLEVENINGSTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLGAPSIDESIDEWHITE = talib.CDLGAPSIDESIDEWHITE(self.df_day['open'].values, self.df_day['high'].values,
                                                                 self.df_day['low'].values, self.df_day['close'].values)
            self.CDLGRAVESTONEDOJI = talib.CDLGRAVESTONEDOJI(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLHAMMER = talib.CDLHAMMER(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLHANGINGMAN = talib.CDLHANGINGMAN(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                     self.df_day['close'].values)
            self.CDLHARAMI = talib.CDLHARAMI(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLHARAMICROSS = talib.CDLHARAMICROSS(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLHIGHWAVE = talib.CDLHIGHWAVE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLHIKKAKE = talib.CDLHIKKAKE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDLHIKKAKEMOD = talib.CDLHIKKAKEMOD(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                     self.df_day['close'].values)
            self.CDLHOMINGPIGEON = talib.CDLHOMINGPIGEON(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLIDENTICAL3CROWS = talib.CDLIDENTICAL3CROWS(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLINNECK = talib.CDLINNECK(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLINVERTEDHAMMER = talib.CDLINVERTEDHAMMER(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLKICKING = talib.CDLKICKING(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDLKICKINGBYLENGTH = talib.CDLKICKINGBYLENGTH(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLLADDERBOTTOM = talib.CDLLADDERBOTTOM(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLLONGLEGGEDDOJI = talib.CDLLONGLEGGEDDOJI(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLLONGLINE = talib.CDLLONGLINE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLMARUBOZU = talib.CDLMARUBOZU(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLMATCHINGLOW = talib.CDLMATCHINGLOW(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLMATHOLD = talib.CDLMATHOLD(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDLMORNINGDOJISTAR = talib.CDLMORNINGDOJISTAR(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLMORNINGSTAR = talib.CDLMORNINGSTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLONNECK = talib.CDLONNECK(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLPIERCING = talib.CDLPIERCING(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLRICKSHAWMAN = talib.CDLRICKSHAWMAN(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLRISEFALL3METHODS = talib.CDLRISEFALL3METHODS(self.df_day['open'].values, self.df_day['high'].values,
                                                                 self.df_day['low'].values, self.df_day['close'].values)
            self.CDLSEPARATINGLINES = talib.CDLSEPARATINGLINES(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLSHOOTINGSTAR = talib.CDLSHOOTINGSTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLSHORTLINE = talib.CDLSHORTLINE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLSPINNINGTOP = talib.CDLSPINNINGTOP(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLSTALLEDPATTERN = talib.CDLSTALLEDPATTERN(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLSTICKSANDWICH = talib.CDLSTICKSANDWICH(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDLTAKURI = talib.CDLTAKURI(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLTASUKIGAP = talib.CDLTASUKIGAP(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLTHRUSTING = talib.CDLTHRUSTING(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLTRISTAR = talib.CDLTRISTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDLUNIQUE3RIVER = talib.CDLUNIQUE3RIVER(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLUPSIDEGAP2CROWS = talib.CDLUPSIDEGAP2CROWS(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLXSIDEGAP3METHODS = talib.CDLXSIDEGAP3METHODS(self.df_day['open'].values, self.df_day['high'].values,
                                                                 self.df_day['low'].values, self.df_day['close'].values)

            # values = []
            # for index, cci in self.df_day['cci'].items():
            #     v = 0
            #     if index >= (len(self.df_o) - 1) or index <= 0:
            #         v = 0
            #     else:
            #         if (cci < self.df_day['cci'].iloc[index + 1]) and (cci < self.df_day['cci'].iloc[index - 1]):
            #             v = -1
            #         elif (cci > self.df_day['cci'].iloc[index + 1]) and (cci > self.df_day['cci'].iloc[index - 1]):
            #             v = 1
            #         else:
            #             v = 0
            #     values.append(v)
            # self.df_day['cci_curve'] = pd.Series(values)

        # 4 weeks
        try:
            self.SMA_200_20 = talib.SMA(self.df_day['close'], timeperiod=200).iloc[-20]
        except:
            self.SMA_200_20 = 0

        self.LOW_52W = self.df_day['low'].head(260).min()
        self.HIGH_52W = self.df_day['high'].head(260).max()

    def __load_finance_info(self):
        # Load finance info
        try:
            sql_finance_info = """select * from tbl_finance_info as ti where ti.code='""" + self.code + """' order by year_period desc, quarter_period desc limit 4"""
            finance_info_data = pd.read_sql_query(sql_finance_info, self.conn)
            self.df_finance = pd.DataFrame(finance_info_data)  # data-frame finance information
        except pd.io.sql.DatabaseError as ex:
            print("No finance information")

    # Load 1000 days = 3 years
    def __load_price_board_day(self, length=1000):
        try:
            sql_resolution_d = """select code, t, o as open, h as high, l as low, c as close, v as volume  from tbl_price_board_day as pb where pb.code='""" + self.code + """' order by t desc limit """ + str(
                length)
            self.df_day = pd.DataFrame(pd.read_sql_query(sql_resolution_d, self.conn))
        except pd.io.sql.DatabaseError as ex:
            print("Something went wrong", ex)

    # Load 6k minute = 100 hours
    def __load_price_board_minute(self, length=6000):
        try:
            sql_resolution_m = """select code, t, o as open, h as high, l as low, c as close, v as volume from tbl_price_board_minute as pb where pb.code='""" + self.code + """' order by t desc limit """ + str(
                length)
            self.df_minute = pd.DataFrame(pd.read_sql_query(sql_resolution_m, self.conn))
        except pd.io.sql.DatabaseError as ex:
            print("Something went wrong")

    def f_get_current_price(self):
        return self.df_day['close'].iloc[-1]

    def f_check_has_value(self):
        return not self.df_day.empty

    # Kiem tra gia tri giao dich trong phien. Toi thieu 3ty, recommend 5ty, best >10ty
    def f_check_gia_tri_giao_dich_trong_phien(self, value=5000000000.0):
        return (not self.df_day.empty and self.LAST_V_SMA_20 * self.CURRENT_CLOSE) >= value

    # last changed
    def f_last_changed(self):
        return self.df_day['changed'].iloc[-1]

    # Check gia chua tang qua 3 phien lien tiep
    def f_check_price_continous_jump(self, step=3):
        return not (self.df_day['close'].iloc[-1] > self.df_day['close'].iloc[-2] > self.df_day['close'].iloc[-3])

    def f_1stRSI(self):
        try:
            return self.df_day['rsi'].iloc[-1]
        except:
            return 0

    def f_2ndRSI(self):
        try:
            return self.df_day['rsi'].iloc[-2]
        except:
            return 0

    def f_3rdRSI(self):
        try:
            return self.df_day['rsi'].iloc[-3]
        except:
            return 0

    def f_1stCCI(self):
        return self.df_day['cci'].iloc[-1]

    def f_cci_3_days_down(self):
        return self.df_day['cci'].iloc[-1] < self.df_day['cci'].iloc[-2] < self.df_day['cci'].iloc[-3]

    def f_2ndCCI(self):
        return self.df_day['cci'].iloc[-2]

    def f_3rdCCI(self):
        return self.df_day['cci'].iloc[-3]

    # upper, middle, lower
    def f_bband(self):
        # close, matype = MA_Type.T3
        return talib.BBANDS(self.df_day['close'], timeperiod=5, matype=talib.MA_Type.T3)

    # macd, macdsignal, macdhist = MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    def f_macd(self):
        return talib.MACD(self.df_day['close'], fastperiod=12, slowperiod=26, signalperiod=9)

    # fastk, fastd = STOCHRSI(close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
    def f_stochrsi(self):
        return talib.STOCHRSI(self.df_day['close'], timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)

    def f_total_vol(self):
        return self.df_day['volume'].iloc[-1]

    # Oversold --> should buy
    def f_is_over_sold(self, horizontal=100):
        return self.cci.iloc[-1] <= (0 - horizontal)

    # Overbought --> should sell
    def f_is_over_bought(self, horizontal=100):
        return self.cci.iloc[-1] > horizontal

    def f_check_uptrend_1_month(self):
        return self.CURRENT_CLOSE > self.df_day['SMA_5'].iloc[-1] > self.df_day['SMA_10'].iloc[-1] > self.df_day['SMA_20'].iloc[-1]

    # check price jump 1%
    def f_check_price_jump(self, step=0.01):
        return self.df_day['changed'].iloc[-1] >= step

    # minervini conditions
    def f_check_7_conditions(self):
        # Condition 1: Current Price > 150 SMA and > 200 SMA
        condition_1 = self.CURRENT_CLOSE > self.df_day['SMA_150'].iloc[-1] > self.df_day['SMA_200'].iloc[-1]

        # Condition 2: 150 SMA and > 200 SMA
        condition_2 = self.df_day['SMA_150'].iloc[-1] > self.df_day['SMA_200'].iloc[-1]

        # Condition 3: 200 SMA trending up for at least 1 month
        condition_3 = self.df_day['SMA_200'].iloc[-1] > self.SMA_200_20

        # Condition 4: 50 SMA> 150 SMA and 50 SMA> 200 SMA
        condition_4 = self.df_day['SMA_50'].iloc[-1] > self.df_day['SMA_150'].iloc[-1] > self.df_day['SMA_200'].iloc[-1]

        # Condition 5: Current Price > 50 SMA
        condition_5 = self.CURRENT_CLOSE > self.df_day['SMA_50'].iloc[-1]

        # Condition 6: Current Price is at least 30% above 52 week low
        condition_6 = self.CURRENT_CLOSE >= (1.3 * self.LOW_52W)

        # Condition 7: Current Price is within 25% of 52 week high
        condition_7 = self.CURRENT_CLOSE >= (.75 * self.HIGH_52W)

        return condition_1 and \
               condition_2 and \
               condition_3 and \
               condition_4 and \
               condition_5 and \
               condition_6 and \
               condition_7

    def f_khoi_luong_giao_dich_tang_dan_theo_so_phien(self, window=5):
        if len(self.df_day) > 5:
            # print(1, self.df['t'].iloc[-1], self.df['volume'].iloc[-1])
            # print(2, self.df['t'].iloc[-2], self.df['volume'].iloc[-2])
            return self.df_day['volume'].iloc[-1] > self.df_day['volume'].iloc[-2] > self.df_day['volume'].iloc[-3]
        else:
            return False

    def f_gia_tang_dan(self, window=5):
        if len(self.df_day) > 5:
            return self.df_day['close'].iloc[-1] > self.df_day['close'].iloc[-2] > self.df_day['close'].iloc[-3]

    def f_check_two_crows(self):
        try:
            res = talib.CDL2CROWS(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values, self.df_day['close'].values)
            return pd.DataFrame({'CDL2CROWS': res}, index=self.df_day.index)
        except:
            return pd.DataFrame({'CDL2CROWS': [-1]})

    def uptrend_ichi(self):
        # up_trends = self.df_day.filter(regex="^(uptrend)_.*").fillna(0)
        # print('dataframe', self.df_day['uptrend_ichimoku'])
        return False

    # ICHIMOKU EVALUATE
    def evaluate_ichimoku5(self, prefix="ichimoku", impact_buy=1, impact_sell=1, ratio=0.1):
        """
        evaluates the ichimoku
        :param ratio:
        :param impact_sell:
        :param impact_buy:
        :param period:
        :param prefix:
        :return:
        """
        print('...EVALUATE_ICHIMOKU5')
        # self._weights(impact_buy, impact_sell)
        # dataframe = self.df_day
        name = '{}'.format(prefix)
        ichimoku = ichimoku5(self.df_day)

        self.df_day['{}_ks9'.format(name)] = ichimoku['ks9']
        self.df_day['{}_ks17'.format(name)] = ichimoku['ks17']
        self.df_day['{}_ks26'.format(name)] = ichimoku['ks26']
        self.df_day['{}_ks65'.format(name)] = ichimoku['ks65']
        self.df_day['{}_ks129'.format(name)] = ichimoku['ks129']
        self.df_day['{}_span_a'.format(name)] = ichimoku['span_a']
        self.df_day['{}_span_b'.format(name)] = ichimoku['span_b']
        self.df_day['{}_span_1'.format(name)] = ichimoku['span_1']
        self.df_day['{}_span_2'.format(name)] = ichimoku['span_2']
        self.df_day['{}_lagging_t2'.format(name)] = ichimoku['lagging_t2']
        self.df_day['{}_lagging_t3'.format(name)] = ichimoku['lagging_t3']

        self.df_day['{}_ks_9_sl_t3'.format(name)] = ichimoku['ks_9_sl_t3']
        self.df_day['{}_ks_17_sl_t3'.format(name)] = ichimoku['ks_17_sl_t3']
        self.df_day['{}_span_2_sl_t2p1'.format(name)] = ichimoku['span_2_sl_t2p1']
        self.df_day['{}_lagging_pt2m1'.format(name)] = ichimoku['lagging_pt2m1']
        self.df_day['{}_lagging_mt2p1'.format(name)] = ichimoku['lagging_mt2p1']

        # UPTREND
        # Uptrend = (KS9 > Ref(Span1,-t3) AND KS9>Ref(Span2,-t3) AND (KS17>Ref(Span1,-t3) AND KS17>Ref(Span2,-t3))) ;
        self.df_day.loc[
            (
                    (self.df_day['{}_ks9'.format(name)] > self.df_day['{}_span_a'.format(name)]) &
                    (self.df_day['{}_ks9'.format(name)] > self.df_day['{}_span_b'.format(name)]) &
                    (self.df_day['{}_ks17'.format(name)] > self.df_day['{}_span_a'.format(name)]) &
                    (self.df_day['{}_ks17'.format(name)] > self.df_day['{}_span_b'.format(name)])

            ),
            'uptrend_{}'.format(name)
        ] = (1 * impact_buy)

        # print('uptrend %s' % self.df_day['uptrend_ichimoku'])

        # DOWNTREND
        # Downtrend=(KS9 < Ref(Span1,-t3) AND KS9<Ref(Span2,-t3) AND (KS17<Ref(Span1,-t3) AND KS17<Ref(Span2,-t3))) ;
        self.df_day.loc[
            (
                    (self.df_day['{}_ks9'.format(name)] < self.df_day['{}_span_a'.format(name)])
                    # (self.df_day['{}_ks9'.format(name)] < self.df_day['{}_span_b'.format(name)]) &
                    # (self.df_day['{}_ks17'.format(name)] < self.df_day['{}_span_a'.format(name)]) &
                    # (self.df_day['{}_ks17'.format(name)] < self.df_day['{}_span_b'.format(name)])
            ),
            'downtrend_{}'.format(name)
        ] = (1 * impact_sell)
        # print('downtrend %s' % self.df_day['downtrend_ichimoku'])
        # print('_ks9 %s' % self.df_day['{}_ks9'.format(name)])
        # print('_span_a %s' % self.df_day['{}_span_1'.format(name)])
        # print('compare %s' % (self.df_day['{}_ks9'.format(name)] < self.df_day['{}_span_a'.format(name)]))

        print('%s' % (self.df_day['{}_span_a'.format(name)]))
        print('%s' % (self.df_day['{}_span_a'.format(name)] < self.df_day['open']))

        # BreakOutAllKuMoCloud- Mua dai han
        # Chikou back26 > Komu va Kumosen hay chilku cat len Kumo va Kumo sen;
        # hoac Chikou 17 >  Komu va Kumosen hay chilku cat len Kumo va Kumo sen;
        # Kijun 9 huong len hoac Kijun 17 huong len;
        # Gia cong cuar lon hon Kjjun 65;
        self.df_day.loc[
            (
                    (  # CD1_UP
                            (self.df_day['{}_lagging_t3'.format(name)] <= self.df_day['{}_span_b'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] <= self.df_day['{}_ks_17_sl_t3'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] <= self.df_day['{}_span_a'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] <= self.df_day['{}_ks_9_sl_t3'.format(name)])
                    ) &
                    (
                        # CD2_UP
                            (self.df_day['{}_lagging_pt2m1'.format(name)] > self.df_day['{}_span_2_sl_t2p1'.format(name)]) &
                            (self.df_day['close'.format(name)] > self.df_day['{}_ks17'.format(name)]) &
                            (self.df_day['close'.format(name)] > self.df_day['{}_span_1'.format(name)]) &
                            (self.df_day['close'.format(name)] > self.df_day['{}_ks9'.format(name)])
                    ) &
                    (  # CD3_UP
                            (self.df_day['{}_ks9'.format(name)] >= (self.df_day['{}_ks9'.format(name)].shift(-1))) |
                            (self.df_day['{}_ks17'.format(name)] >= (self.df_day['{}_ks17'.format(name)].shift(-1)))
                    ) &
                    (  # CD4_UP
                            self.df_day['close'] > self.df_day['{}_ks65'.format(name)]
                    )
            ),
            'breakout_up_{}'.format(name)
        ] = (1 * impact_buy)

        # DOWN
        self.df_day.loc[
            (
                    (  # CD1_DOWN
                            (self.df_day['{}_lagging_t2'.format(name)] >= self.df_day['{}_span_b'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] >= self.df_day['{}_ks_17_sl_t3'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] >= self.df_day['{}_span_a'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] >= self.df_day['{}_ks_9_sl_t3'.format(name)])
                    ) &
                    (  # CD2_DOWN
                            (self.df_day['{}_lagging_pt2m1'.format(name)] < self.df_day['{}_span_2_sl_t2p1'.format(name)]) &
                            (self.df_day['close'.format(name)] < self.df_day['{}_ks17'.format(name)]) &
                            (self.df_day['close'.format(name)] < self.df_day['{}_span_1'.format(name)]) &
                            (self.df_day['close'.format(name)] < self.df_day['{}_ks9'.format(name)])

                    ) &
                    (  # CD3_DOWN
                            (self.df_day['{}_ks9'.format(name)] <= (self.df_day['{}_ks9'.format(name)].shift(-1))) |
                            (self.df_day['{}_ks17'.format(name)] <= (self.df_day['{}_ks17'.format(name)].shift(-1)))
                    ) &
                    (  # CD4_DOWN
                            self.df_day['close'] < self.df_day['{}_ks65'.format(name)]
                    )
            ),
            'breakout_down_{}'.format(name)
        ] = (1 * impact_sell)

        # STRENGTHEN UP
        self.df_day.loc[
            (
                    (((self.df_day['{}_ks9'.format(name)].shift(-1) - self.df_day['{}_ks17'.format(name)].shift(-1)) / self.df_day['{}_ks17'.format(name)].shift(-1)).abs() < ratio) &
                    (((self.df_day['{}_ks9'.format(name)] - self.df_day['{}_ks17'.format(name)]) / self.df_day['{}_ks17'.format(name)]).abs() < ratio) &
                    (self.df_day['{}_ks17'.format(name)] > (self.df_day['{}_ks17'.format(name)].shift(-1))) &
                    (self.df_day['{}_ks65'.format(name)] >= (self.df_day['{}_ks65'.format(name)].shift(-1))) &
                    (self.df_day['close'] >= self.df_day['{}_ks65'.format(name)]) &
                    ((self.df_day['{}_span_1'.format(name)] - self.df_day['{}_span_2'.format(name)]) / self.df_day['{}_span_2'.format(name)]).abs() < 5 * ratio
            ),
            'boom_up_{}'.format(name)
        ] = (1 * impact_buy)

        # STRENGTHEN DOWN
        self.df_day.loc[
            (
                    (((self.df_day['{}_ks9'.format(name)].shift(-1) - self.df_day['{}_ks17'.format(name)].shift(-1)) / self.df_day['{}_ks17'.format(name)].shift(-1)).abs() < ratio) &
                    (((self.df_day['{}_ks9'.format(name)] - self.df_day['{}_ks17'.format(name)]) / self.df_day['{}_ks17'.format(name)]).abs() < ratio) &
                    (self.df_day['{}_ks17'.format(name)] < (self.df_day['{}_ks17'.format(name)].shift(-1))) &
                    (self.df_day['{}_ks65'.format(name)] <= (self.df_day['{}_ks65'.format(name)].shift(-1))) &
                    (self.df_day['close'] >= self.df_day['{}_ks65'.format(name)]) &  # TODO: CHECK?
                    ((self.df_day['{}_span_1'.format(name)] - self.df_day['{}_span_2'.format(name)]) / self.df_day['{}_span_2'.format(name)]).abs() < 5 * ratio
            ),
            'boom_down_{}'.format(name)
        ] = (1 * impact_sell)

        # price is above the cloud
        self.df_day.loc[
            (
                    (self.df_day['{}_span_a'.format(name)] > self.df_day['open']) &
                    (self.df_day['{}_span_b'.format(name)] > self.df_day['open'])
            ),
            'buy_{}'.format(name)
        ] = (1 * impact_buy)

        # price is below the cloud
        self.df_day.loc[
            (
                    (self.df_day['{}_span_a'.format(name)] < self.df_day['open']) &
                    (self.df_day['{}_span_b'.format(name)] < self.df_day['open'])

            ),
            'sell_{}'.format(name)
        ] = (1 * impact_sell)
        print('___EVALUATE_ICHIMOKU5')

    # destructor
    def __del__(self):
        self.conn.close()


# ----------------------------------------------------------------------------- #

########################################
#
# Custom Ichimoku Cloud
#
def ichimoku5(dataframe,
              t1=9,
              t2=17,
              t3=26,
              t4=65,
              t5=129):
    # KS_9
    KS_9 = (dataframe['high'].rolling(window=t1).max()
            + dataframe['low'].rolling(window=t1).min()) / 2

    # KS_17
    KS_17 = (dataframe['high'].rolling(window=t2).max()
             + dataframe['low'].rolling(window=t2).min()) / 2

    # KS_26
    KS_26 = (dataframe['high'].rolling(window=t3).max()
             + dataframe['low'].rolling(window=t3).min()) / 2

    # KS_65
    KS_65 = (dataframe['high'].rolling(window=t4).max()
             + dataframe['low'].rolling(window=t4).min()) / 2

    # KS_129
    KS_129 = (dataframe['high'].rolling(window=t5).max()
              + dataframe['low'].rolling(window=t5).min()) / 2

    SPAN_1 = (KS_9 + KS_17) / 2
    SPAN_2 = (dataframe['high'].rolling(window=t3).max()
              + dataframe['low'].rolling(window=t3).min()) / 2

    SPAN_A = SPAN_1.shift(-t3)
    SPAN_B = SPAN_2.shift(-t3)

    LAGGING_T2 = dataframe['close'].shift(-t2)
    LAGGING_T3 = dataframe['close'].shift(-t3)

    LAGGING_PT2M1 = dataframe['close'].shift(t2 - 1)  # lagging plus (t2 minus 1)
    LAGGING_MT2P1 = dataframe['close'].shift(-(t2 + 1))  # lagging minus (t2 plus 1)

    cloud_green = (SPAN_A > SPAN_B)
    cloud_red = (SPAN_B > SPAN_A)

    KS_17_SL_T3 = KS_17.shift(-t3)  # KS_17 shift left t3
    KS_9_SL_T3 = KS_9.shift(-t3)
    SPAN_2_SL_T2P1 = SPAN_2.shift(-(t2 + 1))  # SPAN 2 shift left t2 + 1

    return {
        'ks9': KS_9,
        'ks17': KS_17,
        'ks26': KS_26,  # extend KS_26
        'ks65': KS_65,  # extend KS_65
        'ks129': KS_129,  # extend KS_129
        'span_a': SPAN_A,
        'span_b': SPAN_B,
        'span_1': SPAN_1,
        'span_2': SPAN_2,
        'lagging_t2': LAGGING_T2,
        'lagging_t3': LAGGING_T3,  # extend
        'cloud_green': cloud_green,
        'cloud_red': cloud_red,
        'ks_9_sl_t3': KS_9_SL_T3,
        'ks_17_sl_t3': KS_17_SL_T3,
        'span_2_sl_t2p1': SPAN_2_SL_T2P1,
        'lagging_pt2m1': LAGGING_PT2M1,
        'lagging_mt2p1': LAGGING_MT2P1,
    }
