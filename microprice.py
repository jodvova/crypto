#!/usr/bin/env python3

import ssl
import os
import sys
import optparse
import websocket
from threading import Thread
import logging
import json
from enum import Enum
from random import randint
import datetime as dt
import time
from math import fabs
import socket
import numpy as np
from numpy_ringbuffer import RingBuffer

WS_TIMEOUT = 5 
BINANCE_URL = "wss://stream.binance.com:9443/ws"
#BINANCE_SNAP_URL = "https://www.binance.com/api/v1/depth?limit=1000&"
BITFINEX_URL = "wss://api.bitfinex.com/ws/2"
GDAX_QA_URL = "wss://ws-feed-public.sandbox.gdax.com"
GDAX_URL = "wss://ws-feed.gdax.com"
LOG_FORMAT = '%(asctime)s|%(name)-8s|%(thread)-6d|%(levelname)-6s|%(message)s'
LOG_FOLDER = "c:/temp"

class log_formatter(logging.Formatter):
    converter=dt.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)
        return s

class channel_state(Enum):
    NONE = 0
    SUBSCRIBING = 1
    SUBSCRIBED = 2
    UNSUBSCRIBING = 3

class market(Enum):
    FINEX = 0
    GDAX = 1
    BINANCE = 2

class channel_data:
    def __init__(self, name):
        self.name_ = name
        self.id_ = 0
        self.status_ = channel_state.NONE

class price_channel(channel_data):
    def __init__(self, name, symbol):
        super().__init__(name)
        self.symbol_ = symbol

class price_level:
    def __init__(self, price, qty, orders):
        self.price_ = price
        self.qty_ = qty
        self.orders_ = orders
    def __str__(self):
        return "%.2f @ %.2f" % (self.qty_, self.price_)

def update_side(arr, price, qty, orders):
    for i in range(0, len(arr)):
        if arr[i].price_ == price:
            if orders == 0: # removal
                arr.pop(i)
                return
            else:
                arr[i] = price_level(price, qty, orders)
                return

class order_book:
    def __init__(self):
        self.bids_ = []
        self.asks_ = []
        self.synced_ = False
        self.open_ = 0.0
        self.low_ = 0.0
        self.high_ = 0.0
        self.last_traded_ = RingBuffer(100, dtype=np.float)

    def is_synced(self):
        return self.synced_
    def top(self):
        return [self.bids_[0] if len(self.bids_) > 0 else [],
                self.asks_[0] if len(self.asks_) > 0 else []]

    def sanity_check(self):
        if len(self.bids_) > 0:
            for i in self.bids_:
                assert i.qty_ > 0
        if len(self.asks_) > 0:
            for i in self.asks_:
                assert i.qty_ > 0
        if len(self.bids_) > 0 and len(self.asks_) > 0:
            assert self.bids_[0].price_ < self.asks_[0].price_
        return True

class finex_order_book(order_book):
    def __init__(self):
        super().__init__()

    def update_internal(self, data):
        assert len(data) == 3
        if data[2] > 0: # bids
            update_side(self.bids_, data[0], data[2], data[1])
        else: # ask
            update_side(self.asks_, data[0], fabs(data[2]), data[1])

    def update(self, data):
        if isinstance(data[0], list):
            for d in data[0]:
                self.update_internal(d)
        else:
            self.update_internal(data)
        
        assert self.sanity_check()
    
    def snapshot(self, data):
        self.bids_ = []
        self.asks_ = []
        for i in data:
            assert len(i) == 3
            if i[2] > 0: # bids
                self.bids_.append(price_level(i[0], i[2], i[1]))
            else: # ask
                self.asks_.append(price_level(i[0], fabs(i[2]), i[1]))
        self.synced_ = True
        
        assert self.sanity_check()

class gdax_order_book(order_book):
    def __init__(self):
        super().__init__()

    def update(self, data):
        for d in data:
            qty = float(d[2])
            if d[0] == 'buy':
                update_side(self.bids_, float(d[1]), qty, 1 if qty != 0 else 0)
            else:
                update_side(self.asks_, float(d[1]), qty, 1 if qty != 0 else 0)
        
        assert self.sanity_check()
    
    def snapshot(self, bids, asks):
        self.bids_ = [ price_level(float(l[0]), float(l[1]), 1) for l in bids ] 
        self.asks_ = [ price_level(float(l[0]), float(l[1]), 1) for l in asks ]
        self.synced_ = True
        assert self.sanity_check()

    def trade(self, data):
        self.open_ = float(data['open_24h'])
        self.low_ = float(data['low_24h'])
        self.high_ = float(data['high_24h'])
        self.last_traded_.append(float(data['price']))

class binance_order_book(order_book):
    def __init__(self):
        super().__init__()

    def update(self, data):
        self.bids_ = [ price_level(float(level[0]), float(level[1]), 1) for level in data['bids'] ]
        self.asks_ = [ price_level(float(level[0]), float(level[1]), 1) for level in data['asks'] ]
        self.synced_ = True
        assert self.sanity_check()
    
class instrument:
    def __init__(self, id, mdsymbols):
        self.id_ = id
        self.mdsymbols_ = mdsymbols
        self.order_books_ = {}
        self.order_books_[market.FINEX] = finex_order_book()
        self.order_books_[market.GDAX] = gdax_order_book()
        self.order_books_[market.BINANCE] = binance_order_book()

    def is_active(self, market):
        return market in self.mdsymbols_

    def id(self):
        return self.id_
    def mdsymbol(self, market):
        return self.mdsymbols_[market]
    def bids(self, market):
        return self.order_books_[market].bids_
    def asks(self, market):
        return self.order_books_[market].asks_
    def top(self, market):
        return self.order_books_[market].top()
    def order_book(self, market):
        return self.order_books_[market]
    
    def average_trade(self, market):
        o = self.order_book(market)
        return np.median(o.last_traded_) if len(o.last_traded_) > 0 else 0.0

    def wpx(self, market):
        if len(self.bids(market)) > 0 and len(self.asks(market)) > 0:
            return (self.bids(market)[0].price_ * self.asks(market)[0].qty_ + self.asks(market)[0].price_ * self.bids(market)[0].qty_) \
                    / (self.bids(market)[0].qty_ + self.asks(market)[0].qty_)
        else:
            return 0.0
    
    def wpx5(self, market):
        if len(self.bids(market)) < 5 or len(self.asks(market)) < 5:
            return 0.0

        a = 0.0
        b = 0.0
        for i in range(0, 5):
            bl = self.bids(market)[i]
            al = self.asks(market)[i]
            a += bl.price_ * al.qty_ + al.price_ * bl.qty_
            b += bl.qty_ + al.qty_
        return a / b

    def to_string(self, market):
        return 'wpx: %.4f wpx5: %.4f traded: %s (%s - %s)' \
            % (self.wpx(market), self.wpx5(market), self.average_trade(market), self.top(market)[0], self.top(market)[1])

class binance:
    def __init__(self, url, products):
        self.market_ = market.BINANCE
        self.uri_ = url
        self.stop_ = False
        self.cid_ = randint(100, 2000)
        self.sockets_ = []
        self.instruments_ = {}
        for i in products:
            if i.is_active(self.market_):
                self.instruments_[i.mdsymbol(self.market_)] = i
        
        self.logger_ = logging.getLogger('binance')
        self.logger_.setLevel(logging.INFO)

    def run(self):
        def run_internal():
            self.connect()
            self.listen()

        self.thread_ = Thread(target=run_internal)
        self.thread_.start()

    def stop(self):
        self.stop_ = True
        if self.thread_:
            self.thread_.join()
            for item in self.sockets_:
                item[0].close()

    def connect(self):
        for k,i in self.instruments_.items():
            url = "%s/%s@depth5" % (self.uri_, i.mdsymbol(self.market_))
            self.logger_.info("connecting: %s" % url)
            ws = websocket.create_connection(url, \
                                            timeout=WS_TIMEOUT, \
                                            sslopt={"cert_reqs": ssl.CERT_NONE }, \
                                            sockopt=((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),))
            self.sockets_.append([ws, i])
            self.handle_connected(ws)

    def listen(self):
        while not self.stop_:
            for item in self.sockets_:
                try:
                    msg = item[0].recv()
                    data = json.loads(msg)
                except json.JSONDecodeError:
                    next
                else:
                    self.handle_partial_book(item[1], data)
    
    def handle_partial_book(self, ins, data):
        self.logger_.debug("received %s" % data)
        ins.order_book(self.market_).update(data)
        self.logger_.debug("price update on %s" % ins.to_string(self.market_))  

    def handle_connected(self, ws):
        self.logger_.info("connected")

class gdax:
    def __init__(self, url, products):
        self.market_ = market.GDAX
        self.uri_ = url
        self.stop_ = False
        self.ws_ = None
        self.instruments_ = {}
        
        for i in products:
            if i.is_active(self.market_):
                self.instruments_[i.mdsymbol(self.market_)] = i
        
        self.logger_ = logging.getLogger('gdax')
        self.logger_.setLevel(logging.INFO)

    def run(self):
        def run_internal():
            self.connect()
            self.listen()

        self.thread_ = Thread(target=run_internal)
        self.thread_.start()

    def stop(self):
        for k,v in self.instruments_.items():
            self.unsubscribe_price(self.ws_, v)
        self.stop_ = True
        if self.thread_:
            self.thread_.join()
            self.ws_.close()

    def connect(self):
        self.logger_.info("connecting: %s" % self.uri_)
        self.ws_ = websocket.create_connection(self.uri_, \
                                               timeout=WS_TIMEOUT, \
                                               sslopt={"cert_reqs": ssl.CERT_NONE }, \
                                               sockopt=((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),))
        self.handle_connected(self.ws_)

    def listen(self):
        ws = self.ws_
        while not self.stop_:
            try:
                msg = ws.recv()
                data = json.loads(msg)
            except json.JSONDecodeError:
                next
            else:
                t = data['type']
                if t == 'error':
                    self.handle_error(ws, data)
                elif t == 'subscriptions':
                    self.handle_subscription(ws, data)
                elif t == 'heartbeat':
                    self.handle_heartbeat(ws, data)
                elif t == 'snapshot':
                    self.handle_snapshot(ws, data)
                elif t == 'l2update':
                    self.handle_l2update(ws, data)
                elif t == 'ticker':
                    self.handle_ticker(ws, data)

    def get_instrument(self, id):
        if not id in self.instruments_:
            raise Exception("Unexpected instrument: %s in snap" % id)
        return self.instruments_[id]

    def handle_ticker(self, ws, data):
        self.logger_.debug("received ticker %s" % data)
        ins = self.get_instrument(data['product_id'])
        ins.order_book(self.market_).trade(data)

    def handle_heartbeat(self, ws, data):
        self.logger_.debug("received hearbeat for %s seq: %s time: %s"  % (data['product_id'], data['sequence'], data['time']))
        id = data['product_id']
        if not id in self.instruments_:
            raise Exception("Unexpected instrument: %s in snap" % id)
        # ins = self.instruments_[id]
        # ins.last_update_ = data['time']
        # ins.sequenuce_ = data['sequence']

    def handle_error(self, ws, data):
        self.logger_.error("error msg received: %s" % data)

    def handle_snapshot(self, ws, data):
        self.logger_.debug("received snap %s" % data)

        id = data['product_id']
        if not id in self.instruments_:
            raise Exception("Unexpected instrument: %s in snap" % id)
        ins = self.instruments_[id]
        ins.order_book(self.market_).snapshot(data['bids'], data['asks'])

    def handle_l2update(self, ws, data):
        self.logger_.debug("received l2data %s" % data)

        ins = self.get_instrument(data['product_id'])
        ins.order_book(self.market_).update(data['changes'])

        self.logger_.debug("price update on %s" % ins.to_string(self.market_))  

    def handle_subscription(self, ws, data):
        self.logger_.info("received subscription update: %s" % data)

    def handle_connected(self, ws):
        self.logger_.info("connected")
        for k,v in self.instruments_.items():
            self.subscribe_price(ws, v)

    def subscribe_price(self, ws, ins):
        self.logger_.debug("subscribing on %s" % ins.id())
        ws.send(json.dumps({ 'type' : 'subscribe', 'product_ids' : ['%s' % ins.mdsymbol(self.market_) ], 'channels' : ['level2', 'heartbeat', 'ticker'] }))

    def unsubscribe_price(self, ws, ins):
        self.logger_.info("unsubscribe from %s" % ins.id())
        ws.send(json.dumps({ 'type' : 'unsubscribe', 'product_ids' : ['%s' % ins.mdsymbol(self.market_) ], 'channels' : ['level2', 'heartbeat', 'ticker'] }))
    
class finex:
    def __init__(self, url, products):
        self.market_ = market.FINEX
        self.uri_ = url
        self.stop_ = False
        self.subscription_ = {}
        self.channel_map_ = {}
        self.cid_ = randint(100, 2000)
        self.ws_ = None
        self.instruments_ = []
        for i in products:
            if i.is_active(self.market_):
                self.instruments_.append(i)

        self.logger_ = logging.getLogger('finex')
        self.logger_.setLevel(logging.INFO)

    def run(self):
        def run_internal():
            self.connect()
            self.listen()

        self.thread_ = Thread(target=run_internal)
        self.thread_.start()

    def stop(self):
        for k,v in self.channel_map_.items():
            self.unsubscribe_channel(self.ws_, k)
        self.stop_ = True
        if self.thread_:
            self.thread_.join()
            self.ws_.close()

    def connect(self):
        self.logger_.info("connecting: %s" % self.uri_)
        self.ws_ = websocket.create_connection(self.uri_, \
                                               timeout=WS_TIMEOUT, \
                                               sslopt={"cert_reqs": ssl.CERT_NONE }, \
                                               sockopt=((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),))
        self.handle_connected(self.ws_)

    def listen(self):
        ws = self.ws_
        while not self.stop_:
            try:
                msg = ws.recv()
                data = json.loads(msg)
            except json.JSONDecodeError:
                next
            else:
                if isinstance(data, dict):
                    t = data['event']
                    if t == 'info':
                        self.handle_info(ws, data)
                    elif t == 'pong':
                        self.handle_pong(ws, data)
                    elif t == 'error':
                        self.handle_error(ws, data)
                    elif t == 'subscribed':
                        self.handle_subscribed(ws, data)
                    elif t == 'unsubscribed':
                        self.handle_unsubscribed(ws, data)
                else:
                    if data[1] == 'hb':
                        self.heartbeat_handler()
                    else:
                        self.data_handler(data)
    
    def heartbeat_handler(self):
        self.logger_.debug("received heartbeat")

    def data_handler(self, data):
        self.logger_.debug("received data on channel: %s" % data)

        # check channel exists
        if not data[0] in self.channel_map_:
            self.logger_.error("received data on unknown channel: %s" % data)
            return
        
        c = self.channel_map_[data[0]]
        if isinstance(c, price_channel):
            ins = c.symbol_
            o = ins.order_book(self.market_)
            if o.is_synced():
                o.update(data[1])
            else:
                o.snapshot(data[1])

            self.logger_.debug("price update on %s" % ins.to_string(self.market_))

    def handle_connected(self, ws):
        self.logger_.info("connected")
        for i in self.instruments_:
            self.subscribe_price(ws, i)
        
    def subscribe_channel(self, ws, channel, kwargs={}):
        d = channel_data(channel)
        self.subscription_[channel] = d
        d.status_ = channel_state.SUBSCRIBING
        self.logger_.debug("subscribing to channel: %s" % d.name_)

        dict1 = { 'event' : 'subscribe', 'channel' : '%s' % channel }
        ws.send(json.dumps({**dict1, **kwargs}))

    def unsubscribe_channel(self, ws, chn_id, delete=False):
        if chn_id in self.channel_map_:
            d = self.channel_map_[chn_id]
            self.logger_.info("unsubscribing from channel: %s id: %s" % (d.name_, d.id_))
            ws.send(json.dumps({ 'event' : 'unsubscribe', 'chanId' : '%s' % d.id_ }))
            if delete:
                del self.channel_map_[d.id_]

    def subscribe_price(self, ws, ins):
        channel = 'book-%s' % ins.id()
        d = price_channel(channel, ins)
        d.status_ = channel_state.SUBSCRIBING
        self.subscription_[channel] = d
        self.logger_.debug("subscribing for price on %s" % ins.id())
        ws.send(json.dumps({ 'event' : 'subscribe', 'channel' : 'book', 'symbol' : '%s' % ins.mdsymbol(self.market_) }))

    def unsubscribe_price(self, ws, chn_id):
        self.unsubscribe_channel(ws, chn_id)

    def handle_subscribed(self, ws, j):
        self.logger_.info("received subscription: %s" % j)
        channel = '%s-%s' % (j['channel'], j['symbol'][1:])
        if not channel in self.subscription_:
            raise Exception("invalid subscription response: %s" % j)

        d = self.subscription_[channel]
        d.status_ = channel_state.SUBSCRIBED
        d.id_ = j['chanId']
        self.channel_map_[d.id_] = d

        self.logger_.info("subscribed to channel: %s id: %s" % (d.name_, d.id_))

    def handle_unsubscribed(self, ws, j):
        if not j['chanId'] in self.channel_map_:
            self.logger_.error("Can't find channel %s" % j['chanId'])
            return        
        d = self.channel_map_[j['chanId']]
        if j['status'] == 'OK':
            self.logger_.info("unsubscribed from channel: %s id: %s" % (d.name_, d.id_))
            del self.subscription_[d.name_]
            del self.channel_map_[d.id_]            
    
    def handle_error(self, ws, j):
        errors = {10000: 'Unknown event',
                  10001: 'Generic error',
                  10008: 'Concurrency error',
                  10020: 'Request parameters error',
                  10050: 'Configuration setup failed',
                  10100: 'Failed authentication',
                  10111: 'Error in authentication request payload',
                  10112: 'Error in authentication request signature',
                  10113: 'Error in authentication request encryption',
                  10114: 'Error in authentication request nonce',
                  10200: 'Error in un-authentication request',
                  10300: 'Subscription Failed (generic)',
                  10301: 'Already Subscribed',
                  10302: 'Unknown channel',
                  10400: 'Subscription Failed (generic)',
                  10401: 'Not subscribed',
                  11000: 'Not ready, try again later',
                  20000: 'User is invalid!',
                  20051: 'Websocket server stopping',
                  20060: 'Websocket server resyncing',
                  20061: 'Websocket server resync complete'
                  }
        self.logger_.error(errors[j['code']])

    def handle_info(self, ws, j):
        self.logger_.info("%s" % j)

    def handle_pong(self, ws, j):
        self.logger_.debug("pong received")    
    
    def ping(self, ws):
        msg = json.dumps({ 'event' : 'ping' })
        self.logger_.debug("sent ping: %s" % msg)
        ws.send(msg)

def print_state(logger, instruments):
    for i in instruments:
        if i.is_active(market.FINEX):
            logger.info("%s (%s) : %s" % (i.id(), market.FINEX, i.to_string(market.FINEX)))
        if i.is_active(market.GDAX):
            logger.info("%s (%s): %s" % (i.id(), market.GDAX, i.to_string(market.GDAX)))
        if i.is_active(market.BINANCE):
            logger.info("%s (%s): %s" % (i.id(), market.BINANCE, i.to_string(market.BINANCE)))

if __name__ == "__main__":
    usage = "usage: %prog [options]"
    opt = optparse.OptionParser(usage=usage, version="%prog 1.0")
    (options, args) = opt.parse_args()

    consoleLogger = logging.StreamHandler()
    consoleLogger.setFormatter(log_formatter(fmt=LOG_FORMAT,datefmt='%H:%M:%S.%f'))
    logging.basicConfig(handlers=[consoleLogger])
    
    # setup websockets logger
    logging.getLogger('websockets').setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create instruments
    instruments = [ instrument("BTCUSD", { market.FINEX : "BTCUSD", market.GDAX : "BTC-USD", market.BINANCE : "btcusdt" }), \
                    instrument("ETHUSD", { market.FINEX : "ETHUSD", market.GDAX : "ETH-USD", market.BINANCE : "ethusdt" }) ]

    try:
        f = finex(BITFINEX_URL, instruments)
        g = gdax(GDAX_URL, instruments)      
        b = binance(BINANCE_URL, instruments)
        f.run()
        g.run()
        b.run()
        while True:
            time.sleep(2)
            print_state(logger, instruments)
    except KeyboardInterrupt:
        logger.info("stop requested")
        b.stop()
        g.stop()
        f.stop()
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        import traceback
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        b.stop()
        g.stop()
        f.stop()