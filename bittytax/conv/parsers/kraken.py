# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2020
import sys

from decimal import Decimal

from ...config import config
from ..out_record import TransactionOutRecord
from ..exceptions import UnexpectedTypeError, UnexpectedTradingPairError, DataRowError, UnexpectedTypeError, \
    MissingComponentError
from ..dataparser import DataParser
from colorama import Fore, Back

WALLET = "Kraken"

QUOTE_ASSETS = ['AUD', 'CAD', 'CHF', 'DAI', 'DOT', 'ETH', 'EUR', 'GBP', 'JPY', 'USD',
                'USDC', 'USDT', 'XBT', 'XETH', 'XXBT', 'ZAUD', 'ZCAD', 'ZEUR', 'ZGBP', 'ZJPY',
                'ZUSD']

ALT_ASSETS = {'KFEE': 'FEE', 'XETC': 'ETC', 'XETH': 'ETH', 'XLTC': 'LTC', 'XMLN': 'MLN',
              'XREP': 'REP', 'XXBT': 'XBT', 'XXDG': 'XDG', 'XXLM': 'XLM', 'XXMR': 'XMR',
              'XXRP': 'XRP', 'XZEC': 'ZEC', 'ZAUD': 'AUD', 'ZCAD': 'CAD', 'ZEUR': 'EUR',
              'ZGBP': 'GBP', 'ZJPY': 'JPY', 'ZUSD': 'USD',
              'ADA.S': 'ADA', 'DOT.S': 'DOT', 'POL.S': 'POL', 'SOL.S': 'SOL', 'ALGO.S': 'ALGO', 'LUNA.S': 'LUNA'}

ASSETS_2CHARS = ['SC']
TRANSACTION_TYPES = {"deposit": TransactionOutRecord.TYPE_DEPOSIT,
                     "staking": TransactionOutRecord.TYPE_STAKING,
                     "dividend": TransactionOutRecord.TYPE_DIVIDEND,
                     "reward": TransactionOutRecord.TYPE_STAKING}
TRADE_TYPE_PAIR = {"trade": "trade",
                   "spend": "receive",
                   "receive": "spend"}


def parse_kraken_all(data_rows, parser, **_kwargs):
    trade_ids = {}
    for dr in data_rows:
        row_dict = dr.row_dict
        tx_id = row_dict['txid']
        if tx_id == "":
            continue
        ref_id = row_dict['refid']
        if ref_id in trade_ids:
            trade_ids[ref_id].append(dr)
        else:
            trade_ids[ref_id] = [dr]

    for data_row in data_rows:
        if config.debug:
            sys.stderr.write("%sconv: row[%s] %s\n" % (
                Fore.YELLOW, parser.in_header_row_num + data_row.line_num, data_row))

        if data_row.parsed:
            continue

        try:
            parse_kraken_deposits_withdrawals(trade_ids, parser, data_row)
        except DataRowError as e:
            data_row.failure = e
    pass


def parse_kraken_deposits_withdrawals(trade_ids, parser, data_row):
    data_row.parsed = True
    row_dict = data_row.row_dict
    time = row_dict['time']
    data_row.timestamp = DataParser.parse_timestamp(time, dayfirst=False)
    transaction_type = row_dict['type']
    tx_id = row_dict['txid']
    ref_id = row_dict['refid']
    if tx_id == "": return
    if transaction_type in ["deposit", "staking", "reward", "dividend"]:
        # Check for txid to filter failed transactions
        data_row.t_record = TransactionOutRecord(TRANSACTION_TYPES[transaction_type],
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['amount'],
                                                 buy_asset=normalise_asset(row_dict['asset']),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=WALLET)
    elif transaction_type in ["trade", "spend", "receive"]:
        # Check for txid to filter failed transactions
        data_row2 = get_trade(trade_ids[ref_id], TRADE_TYPE_PAIR[transaction_type])
        if data_row2 is None:
            raise MissingComponentError(parser.in_header.index('trade id'), 'trade id',
                                        row_dict['trade id'])
        rd1 = data_row.row_dict
        rd2 = data_row2.row_dict
        amount1 = Decimal(rd1['amount'])
        amount2 = Decimal(rd2['amount'])
        asset1 = normalise_asset(rd1['asset'])
        asset2 = normalise_asset(rd2['asset'])
        # 2nd value has it always 0
        fee1 = Decimal(rd1['fee'])
        fee2 = Decimal(rd2['fee'])
        if fee1 > 0.0 and fee2 > 0.0:
            raise "Unhandled case of fee1 && fee2 are both not 0.0"

        fee_quantity = fee1 + fee2

        sell_quantity = abs(amount1) if amount1 < 0.0 else abs(amount2)
        sell_asset = asset1 if amount1 < 0.0 else asset2
        buy_quantity = abs(amount1) if amount1 > 0.0 else abs(amount2)
        buy_asset = asset1 if amount1 > 0.0 else asset2
        fee_asset = asset1 if fee1 > 0.0 else asset2

        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=buy_quantity,
                                                 buy_asset=buy_asset,
                                                 sell_quantity=sell_quantity,
                                                 sell_asset=sell_asset,
                                                 fee_quantity=fee_quantity,
                                                 fee_asset=fee_asset,
                                                 wallet=WALLET)
    elif transaction_type == "withdrawal":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=abs(Decimal(row_dict['amount'])),
                                                 sell_asset=normalise_asset(row_dict['asset']),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(row_dict['asset']),
                                                 wallet=WALLET)
    elif transaction_type == "transfer":
        # in kraken transfer e.g. default -> staking
        pass
    elif tx_id != "":
        sys.stderr.write(
            "%sWARNING%s Unsupported type: 'Kraken:%s'. Audit will not match.%s\n" % (
                Back.YELLOW + Fore.BLACK, Back.RESET + Fore.YELLOW, transaction_type, Fore.RESET))


def get_trade(trade_id_rows, t_type):
    quantity = None
    asset = ""

    for data_row in trade_id_rows:
        if not data_row.parsed and t_type == data_row.row_dict['type']:
            quantity = abs(Decimal(data_row.row_dict['amount']))
            data_row.timestamp = DataParser.parse_timestamp(data_row.row_dict['time'], dayfirst=True)
            data_row.parsed = True
            return data_row

    return None


def parse_kraken_trades(data_row, parser, **_kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['time'])

    base_asset, quote_asset = split_trading_pair(row_dict['pair'])
    if base_asset is None or quote_asset is None:
        raise UnexpectedTradingPairError(parser.in_header.index('pair'), 'pair', row_dict['pair'])

    if row_dict['type'] == "buy":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['vol'],
                                                 buy_asset=normalise_asset(base_asset),
                                                 sell_quantity=row_dict['cost'],
                                                 sell_asset=normalise_asset(quote_asset),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(quote_asset),
                                                 wallet=WALLET)
    elif row_dict['type'] == "sell":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['cost'],
                                                 buy_asset=normalise_asset(quote_asset),
                                                 sell_quantity=row_dict['vol'],
                                                 sell_asset=normalise_asset(base_asset),
                                                 fee_quantity=row_dict['fee'],
                                                 fee_asset=normalise_asset(quote_asset),
                                                 wallet=WALLET)
    else:
        raise UnexpectedTypeError(parser.in_header.index('type'), 'type', row_dict['type'])


def split_trading_pair(trading_pair):
    for quote_asset in sorted(QUOTE_ASSETS, reverse=True):
        if trading_pair.endswith(quote_asset) and (len(trading_pair) - len(quote_asset) >= 3 \
                                                   or trading_pair[:2] in ASSETS_2CHARS):
            return trading_pair[:-len(quote_asset)], quote_asset

    return None, None


def normalise_asset(asset):
    if asset in ALT_ASSETS:
        asset = ALT_ASSETS.get(asset)

    asset = asset.replace(".S", "")

    if asset == "XBT":
        return "BTC"
    return asset


DataParser(DataParser.TYPE_EXCHANGE,
           "Kraken Deposits/Withdrawals",
           ['txid', 'refid', 'time', 'type', 'subtype', 'aclass', 'asset', 'amount', 'fee',
            'balance'],
           worksheet_name="Kraken D,W",
           all_handler=parse_kraken_all)

DataParser(DataParser.TYPE_EXCHANGE,
           "Kraken Trades",
           ['txid', 'ordertxid', 'pair', 'time', 'type', 'ordertype', 'price', 'cost', 'fee', 'vol',
            'margin', 'misc', 'ledgers', 'postxid', 'posstatus', 'cprice', 'ccost', 'cfee', 'cvol',
            'cmargin', 'net', 'trades'],
           worksheet_name="Kraken T",
           row_handler=parse_kraken_trades)

DataParser(DataParser.TYPE_EXCHANGE,
           "Kraken Trades",
           ['txid', 'ordertxid', 'pair', 'time', 'type', 'ordertype', 'price', 'cost', 'fee', 'vol',
            'margin', 'misc', 'ledgers'],
           worksheet_name="Kraken T",
           row_handler=parse_kraken_trades)
