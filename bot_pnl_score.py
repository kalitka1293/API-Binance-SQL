from binance.client import Client
import time
import datetime
import sqlite3
from sql import name_db
import asyncio

class Bot_pnl:
    def __init__(self, name_db):
        conn = sqlite3.connect(f'{name_db}.db', check_same_thread=False) # Перед тем как вносить в проект добавить
        # conn = sqlite3.connect(rf'C:\Users\user\Desktop\{name_db}.db', check_same_thread=False)
        self.conn = conn
    def binance(self):
        cur = self.conn.cursor()
        cur.execute('SELECT option_id, public, secret, user_id FROM users;')
        api_user = cur.fetchall() #API Подписчиков (id, API)
        symbol = self.symbol()
        full_data = {}
        for i in api_user:
            key = i[1]
            api_secret = i[2]
            user_id = i[3]
            try:
                client = Client(key, api_secret) #Создаем обьект для каждого пользователя
                for pair in symbol:
                    option_id = i[0]
                    try:
                        print(type(pair[0]))
                        print(pair[0])
                        data = client.futures_account_trades(symbol=pair[0])
                        time.sleep(0.1)
                        full_data.update({user_id:[{option_id:data}]}) #Сохраняем собранные данные
                    except Exception as f:
                        print('data pair', f)
            except Exception as f:
                print('еще одна ошибка бялть', f)            
        return full_data

    def symbol(self):
        cur = self.conn.cursor()
        try:
            cur.execute('SELECT pair FROM symbol;')
            print(cur.fetchall())
            return cur.fetchall()
        except Exception as f:
            print('SYMBOL', f)

    def score_pnl(self):
        now_time_sec = time.time_ns() // 1000000
        one_day_sec = 87400000
        yesterday_time_sec = now_time_sec - one_day_sec # Время в милисекундах
        
        id_pnl = {}
        
        data = self.binance()
        
        for key in data.keys():
            spis_pnl = []
            for data_dict in data[key]:
                for user_id in data_dict.keys():
                    for d in data_dict[user_id]:
                        if 'realizedPnl' in d:
                            time_order = d['time']
                            if now_time_sec >= time_order and time_order >= yesterday_time_sec:
                                pnl = d['realizedPnl']
                                spis_pnl.append(float(pnl))
                    id_pnl.update({key:[{user_id:sum(spis_pnl)}]})

        return id_pnl

    def send_pnl_id(self):
        id_pnl = self.score_pnl()
        cur = self.conn.cursor()
        for key, value in id_pnl.items():
            for options_id, pnl in value[0].items():
                cur.execute('SELECT conn_users, pnl FROM options WHERE option_id={};'.format(options_id))
                x = cur.fetchall()
                id = list(filter(None, x[0][0].split('|')))
                for i in id:
                    print('id= {}\npnl= {}\n%pnl= {}\nСумма к оплате = {}\n'.format(i, pnl, x[0][1], pnl / x[0][1]))
        return id_pnl



    def conn_close(self):
        self.conn.close()
    #Удалить потом
    def send_message(self):
        x = 2**12312
        return x


#{2115197756: [{'13': 33.20298994}], 5060407770: [{'13': 33.20298994}], 219229444: [{'11': -0.59128}]}