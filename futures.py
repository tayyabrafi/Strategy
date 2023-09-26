import json
import logging
import typing
import requests
import time
from pprint import pprint

from models import *

from urllib.parse import urlencode

import hmac
import hashlib

import websocket
import threading

from strategies import TechnicalStrategy, BreakoutStrategy


logger = logging.getLogger()


class BinanceFuturesClient:
    def __init__(self, public_key: str, secret_key: str, testnet: bool, futures: bool):

        self.futures = futures

        if self.futures:
            self.platform = "binance_futures"
            if testnet:
                self._base_url = "https://testnet.binancefuture.com"
                self._wss_url = "wss://stream.binancefuture.com/ws"
            else:
                self._base_url = "https://fapi.binance.com"
                self._wss_url = "wss://fstream.binance.com/ws"
        else:
            self.platform = "binance_spot"
            if testnet:
                self._base_url = "https://testnet.binance.vision"
                self._wss_url = "wss://testnet.binance.vision/ws"
            else:
                self._base_url = "https://api.binance.com"
                self._wss_url = "wss://stream.binance.com:9443/ws"

        self.public_key = public_key
        self.secret_key = secret_key

        self.headers = {'X-MBX-APIKEY': self.public_key}
        self.prices = dict()

        self.contracts = self.get_contracts()
        self.balances = self.get_balances()

        self.strategies: typing.Dict[int, typing.Union[TechnicalStrategy, BreakoutStrategy]] = dict()

        self.var = 1
        self.logs = []

        self._ws_id = 1
        self.ws: websocket.WebSocketApp
        self.reconnect = True

        t = threading.Thread(target=self.start_ws)
        t.start()

        logger.info('Binance Future Client has been initialized')

    def add_log(self, msg: str):
        logger.info('%s', msg)
        self.logs.append({'log': msg, 'displayed': False})

    def generate_signature(self, data: typing.Dict) -> str:
        return hmac.new(self.secret_key.encode(), urlencode(data).encode(), hashlib.sha256).hexdigest()

    def make_request(self, method: str, endpoint: str, data: typing.Dict):
        if method == 'GET':
            try:
                response = requests.get(self._base_url + endpoint, params=data, headers=self.headers)
            except Exception as e:
                logger.error("Connection error while making %s request to %s: %s", method, endpoint, e)
                return None

        elif method == 'POST':
            try:
                response = requests.post(self._base_url + endpoint, params=data, headers=self.headers)
            except Exception as e:
                logger.error("Connection error while making %s request to %s: %s", method, endpoint, e)
                return None

        elif method == 'DELETE':
            try:
                response = requests.delete(self._base_url + endpoint, params=data, headers=self.headers)
            except Exception as e:
                logger.error("Connection error while making %s request to %s: %s", method, endpoint, e)
                return None
        else:
            raise ValueError()

        if response.status_code == 200:
            return response.json()
        else:
            logger.error("Error while making %s request to %s: %s (error code %s)",
                         method, endpoint, response.json(), response.status_code)

    def get_contracts(self) -> typing.Dict[str, Contract]:
        data = dict()
        if self.futures:
            exchange_info = self.make_request('GET', '/fapi/v1/exchangeInfo', data)
        else:
            exchange_info = self.make_request('GET', '/api/v3/exchangeInfo', data)

        contracts = dict()

        if exchange_info is not None:
            for contract_data in exchange_info['symbols']:
                if contract_data['marginAsset'] != 'BUSD':
                    contracts[contract_data['symbol']] = Contract(contract_data)

            return contracts

    def get_historical_candles(self, contract: Contract, interval: str) -> typing.List[Candle]:
        data = dict()
        data['symbol'] = contract.symbol
        data['interval'] = interval
        data['limit'] = 1000

        if self.futures:
            raw_candles = self.make_request('GET', '/fapi/v1/klines', data)
        else:
            raw_candles = self.make_request('GET', '/api/v3/klines', data)

        candles = []

        if raw_candles is not None:
            for c in raw_candles:

                candles.append(Candle(c, "binance", interval))

        # candles is a list of objects of class Candle which contains(high, low, volume, etc.)
        # each object has a candle containing values of high, low, open, close of that particular timeframe
        return candles

    def get_bid_ask(self, contract: Contract) -> typing.Dict[str, float]:
        data=dict()
        data['symbol'] = contract.symbol

        if self.futures:
            ob_data = self.make_request('GET', '/fapi/v1/ticker/bookTicker', data)
        else:
            ob_data = self.make_request('GET', '/api/v3/ticker/bookTicker', data)

        if ob_data is not None:
            if contract.symbol not in self.prices:
                self.prices[contract.symbol]={'bid': float(ob_data['bidPrice']), 'ask': float(ob_data['askPrice'])}
            else:
                self.prices[contract.symbol]['bid']= float(ob_data['bidPrice'])
                self.prices[contract.symbol]['ask']= float(ob_data['askPrice'])

        return self.prices[contract.symbol]

    def get_balances(self):
        data = dict()
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self.generate_signature(data)

        balances = dict()

        if self.futures:
            balance_data = self.make_request('GET', '/fapi/v2/account', data)
        else:
            balance_data = self.make_request('GET', '/api/v3/account', data)


        for b in balance_data['assets']:
            balances[b['asset']] = Balance(b)

        return balances

    def place_order(self, contract: Contract, order_type: str, quantity: float, side: str, price=None, tif=None):
        data = dict()
        data['symbol'] = contract.symbol
        data['side'] = side.upper()
        data['quantity'] = quantity
        data['type'] = order_type

        if price is not None:
            data['price']=price
        if tif is not None:
            data['timeInForce']=tif

        data['timestamp']=int(time.time() * 1000)
        data['signature']=self.generate_signature(data)

        if self.futures:
            order_status = self.make_request('POST', '/fapi/v1/order', data)
        else:
            order_status = self.make_request('POST', '/api/v3/order', data)

        if order_status is not None:
            order_status = OrderStatus(order_status)

        return order_status

    def cancel_order(self, contract: Contract, order_id: str):
        data=dict()
        data['symbol']= contract.symbol
        data['orderId']= order_id

        data['timestamp']=int(time.time()*1000)
        data['signature']=self.generate_signature(data)

        if self.futures:
            order_status = self.make_request('DELETE', '/fapi/v1/order', data)
        else:
            order_status = self.make_request('DELETE', '/api/v3/order', data)

        if order_status is not None:
            order_status = OrderStatus(order_status)
            logger.info('The order with order_id:%s han been canceled', order_id)
        return order_status

    def get_order_status(self, contract: Contract, order_id: str):
        data=dict()
        data['orderId'] = order_id
        data['symbol']= contract.symbol
        data['timestamp'] = int(time.time()*1000)
        data['signature'] = self.generate_signature(data)

        if self.futures:
            order_status = self.make_request('GET', '/fapi/v1/order', data)
        else:
            order_status = self.make_request('GET', '/api/v3/order', data)

        if order_status is not None:
            order_status = OrderStatus(order_status)
        return order_status

    def get_open_orders(self):
        data=dict()
        data['timestamp']=int(time.time()*1000)
        data['signature']=self.generate_signature(data)

        open_orders=dict()

        if self.futures:
            orders = self.make_request('GET', '/fapi/v1/openOrders', data)
        else:
            orders = self.make_request('GET', '/api/v3/openOrders', data)

        for order in orders:
            open_orders[order['orderId']]=[order['symbol'], order['price'], order['side'], order['origQty'], order['type']]
        #pprint(orders)
        return open_orders

    def start_ws(self):
        # starts a connection and assigns which function is triggered when an event occurs
        # callback functions/methods (on_open, on_close, on_error..)

        self.ws = websocket.WebSocketApp(self._wss_url, on_open=self.on_open, on_close=self.on_close,
                                         on_error=self.on_error, on_message=self.on_message)

        while True:
            try:
                if self.reconnect:
                    self.ws.run_forever()
                    # it is an infinite loop waiting for messages from the server
                else:
                    break
            except Exception as e:
                logger.info('Binance error in run_forever() method: %s', e)
            time.sleep(2)

    def on_open(self, ws):
        logger.info('Binance Websocket connection opened')
        #self.subscribe_channel('BTCUSDT', 'aggTrade')

    def on_close(self, ws):
        logger.warning('Binance Websocket connection closed')

    def on_error(self, ws, msg):
        logger.error('Binance connection error: %s', msg)

    def on_message(self, ws, msg: str):
        data = json.loads(msg)

        if "e" in data:  # value of ['e'] is name of the channel
            if data['e'] == "bookTicker":

                symbol = data['s']

                if symbol not in self.prices:
                    self.prices[symbol] = {'bid': float(data['b']), 'ask': float(data['a'])}
                else:
                    self.prices[symbol]['bid'] = float(data['b'])
                    self.prices[symbol]['ask'] = float(data['a'])

                # PNL Calculation
                try:
                    for b_index, strat in self.strategies.items():
                        if strat.contract.symbol == symbol:
                            for trade in strat.trades:
                                if trade.status == "open" and trade.entry_price is not None:
                                    if trade.side == "long":
                                        trade.pnl = (self.prices['symbol']['bid'] - trade.entry_price) * trade.quantity
                                    elif trade.side == "short":
                                        trade.pnl = (trade.entry_price - self.prices['symbol']['ask']) * trade.quantity
                except RuntimeError as e:
                    logger.error("Error while looping through the Binance strategies: %s", e)

            if data['e'] == "aggTrade":

                symbol = data['s']

                for key, strat in self.strategies.items():

                    if strat.contract.symbol == symbol:
                        res = strat.parse_trade(float(data['p']), float(data['q']), data['T'])

                        strat.check_trade(res)

    def subscribe_channel(self, contracts: typing.List[Contract], channel: str):
        data = dict()
        data['method'] = "SUBSCRIBE"
        data['params'] = []

        for contract in contracts:
            data['params'].append(contract.symbol.lower() + "@" + channel)
        data['id'] = self._ws_id

        try:
            self.ws.send(json.dumps(data))
        except Exception as e:
            logger.error("Websocket error while subscribing to %s %s updates: %s", len(contracts), channel, e)

        self._ws_id += 1

    def get_trade_size(self, contract: Contract, price: float, balance_pct: float):

        balance = self.get_balances()

        if balance is not None:
            if 'USDT' in balance:
                balance = balance['USDT'].wallet_balance
            else:
                return None
        else:
            return None
        logger.info("Binance Futures current USDT balance = %s (%s)", balance, type(balance))
        print(type(price))
        print(type(balance))
        print(type(balance_pct))
        trade_size = (balance * (balance_pct / 100)) / price
        print(trade_size)

        trade_size = round(round(trade_size / contract.lot_size) * contract.lot_size, 8)

        logger.info("Binance Futures current USDT balance = %s, trade size = %s", balance, trade_size)

        return trade_size
