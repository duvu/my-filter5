import pymysql
import pandas as pd
from datetime import datetime

from stock import Stock

if __name__=='__main__':
    print('Hello ...')
    conn = pymysql.connect(host='localhost', user='admin', password='123456', database='mystocks')
    cursor = conn.cursor()
    # sql_query = pd.read_sql_query('''select * from price_board as pb where pb.t_code="FPT"''', conn)
    sql_query = pd.read_sql_query('''select * from tbl_company where Exchange='HOSE' or Exchange='HNX' or Exchange='Upcom' order by Code ASC''', conn)
    #
    df = pd.DataFrame(sql_query)
    # print('df', df)
    # df['stock'] = Stock(df['code'])
    # for (i, code, stock) in df.iterrows():
    #     print(code, stock)
    # _session = datetime.now().date()
    # s = Stock('FPT')
    # print(s.f_check_two_crows()['CDL2CROWS'].iloc[-1])
    # isTop = s.f_is_current_possible_top(window=3)
    # isBot = s.f_is_current_possible_bottom(window=3)
    # print('isTop', isTop)
    # print('isBot', isBot)

    # s.consensus_day.evaluate_rsi()
    # s.consensus_day.evaluate_cci()
    # s.consensus_day.evaluate_ichimoku(impact_buy=1, impact_sell=1)
    # s.consensus_day.evaluate_stoch()
    # s.consensus_day.evaluate_macd_cross_over()
    # s.consensus_day.evaluate_macd()
    # s.consensus_day.evaluate_hull()
    # s.consensus_day.evaluate_vwma()
    # s.consensus_day.evaluate_tema(period=9)
    # s.consensus_day.evaluate_tema(period=20)
    # s.consensus_day.evaluate_tema(period=50)
    # s.consensus_day.evaluate_ema(period=9)
    # s.consensus_day.evaluate_sma(period=20)
    # s.consensus_day.evaluate_laguerre()
    # s.consensus_day.evaluate_osc()
    # s.consensus_day.evaluate_cmf()
    # s.consensus_day.evaluate_cmo()
    # s.consensus_day.evaluate_ultimate_oscilator()
    # s.consensus_day.evaluate_williams()
    # s.consensus_day.evaluate_momentum()
    # s.consensus_day.evaluate_adx()

    # print("Should BUY: ", s.consensus_day.score()['buy_agreement'].iloc[-1], s.consensus_day.score()['buy_disagreement'].iloc[-1])
    # print("Should SELL", s.consensus_day.score()['sell_agreement'].iloc[-1], s.consensus_day.score()['sell_disagreement'].iloc[-1])

    buy_rows = []
    sell_rows = []
    for idx, code in df['Code'].iteritems():
        try:
            s = Stock(code=code)
        except Exception as ex:
            print('Exception', ex)
            continue
    #     # if s.f_check_7_conditions():
    #     #     print('Good code: ', code)
    #
    #     # April 7
    #     # if s.f_check_has_value() and s.f_check_gia_tri_giao_dich_trong_phien() and s.f_check_uptrend_1_month() and s.f_check_price_jump() and s.f_check_price_continous_jump():
    #     #     print(s.LAST_SESSION, "Good to buy: ", code, s.f_total_vol(), "last CCI: ", s.f_1stCCI())
    #     #     rows.append([s.LAST_SESSION, code, s.f_total_vol(), s.EPS, s.EPS_MEAN4, s.f_1stCCI(), s.f_get_current_price(), s.f_last_changed()])
    #
    #     # if s.f_is_current_possible_bottom() and s.f_1stCCI() < -100 and s.f_check_gia_tri_giao_dich_trong_phien():
    #     #     rows.append([s.LAST_SESSION, code, s.f_total_vol(), s.EPS, s.EPS_MEAN4, s.f_1stCCI(), s.f_last_changed()])
    #
    #     # April 9
    #     # check gia giao dong 5 phien gan day
    #     # if s.f_khoi_luong_giao_dich_tang_dan_theo_so_phien() and s.EPS > 0 and s.f_check_gia_tri_giao_dich_trong_phien() and s.EPS_MEAN4 > 1000:
    #     #     rows.append([s.LAST_SESSION, code, s.f_total_vol(), s.EPS, s.EPS_MEAN4, s.f_1stCCI(), s.f_get_current_price(), s.f_last_changed()])
    #
    #     # CDL2ROWS patterns
    #     if hasattr(s, 'CDLDOJISTAR') and s.CDLDOJISTAR[-1] > 0:
    #         rows.append([s.LAST_SESSION, code, s.f_total_vol(), s.EPS, s.EPS_MEAN4, s.f_1stCCI(), s.f_get_current_price(), s.f_last_changed()])
        # Ichimoku
        s.consensus_day.evaluate_ichimoku()
        s_score = s.consensus_day.score()
        buy_agreement = s_score['buy_agreement'].iloc[-1]
        buy_disagreement = s_score['buy_disagreement'].iloc[-1]

        sell_agreement = s_score['sell_agreement'].iloc[-1]
        sell_disagreement = s_score['sell_disagreement'].iloc[-1]

        if buy_agreement > buy_disagreement and s.f_check_gia_tri_giao_dich_trong_phien():
            print('BUY BUY BUY', code)
            buy_rows.append([s.LAST_SESSION, code, s.f_total_vol(), s.EPS, s.EPS_MEAN4, s.f_1stCCI(), s.f_get_current_price(), s.f_last_changed() * 100, buy_agreement, buy_disagreement])

        if sell_agreement > sell_disagreement and s.f_check_gia_tri_giao_dich_trong_phien():
            print('SELL SELL SELL', code)
            sell_rows.append([s.LAST_SESSION, code, s.f_total_vol(), s.EPS, s.EPS_MEAN4, s.f_1stCCI(), s.f_get_current_price(), s.f_last_changed() * 100, sell_agreement, sell_disagreement])
        # IMPORTANT after a loop, need to delete s
        del s

    results_buy = pd.DataFrame(buy_rows, columns=["Session", "Code", "Volume", "EPS", "EPS_MEAN4", "CCI", 'Price', 'Changed', 'Agree', 'Disagree'])
    sheetName = datetime.now().strftime("%b%d")
    results_buy.to_excel("outputs/outputs_buy" + sheetName + ".xlsx", sheet_name=sheetName)

    results_sell = pd.DataFrame(sell_rows, columns=["Session", "Code", "Volume", "EPS", "EPS_MEAN4", "CCI", 'Price', 'Changed', 'Agree', 'Disagree'])
    sheetName = datetime.now().strftime("%b%d")
    results_buy.to_excel("outputs/outputs_sell" + sheetName + ".xlsx", sheet_name=sheetName)
