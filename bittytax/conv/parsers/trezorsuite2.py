# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2021

import re

from decimal import Decimal

from ..out_record import TransactionOutRecord
from ..dataparser import DataParser
from ..exceptions import UnknownCryptoassetError, UnexpectedTypeError, DataParserError

WALLET = "Trezor"


def parse_trezor_suite_new(data_row, parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(int(row_dict['Timestamp']), dayfirst=True, tz='Europe/London')
    symbol = row_dict['Amount unit']
    fee_symbol = row_dict['Fee unit']

    if row_dict['Type'] == "RECV":
        # Workaround: we have to ignore the fee as fee is for the sender
        if Decimal(row_dict['Amount']) == 0.0:
            matches = re.findall(r"\((.*) (.*)\)", row_dict['Addresses'])[0]
            if matches is None:
                raise DataParserError(kwargs['filename'], kwargs.get('worksheet'))
            quantity = Decimal(matches[0])
            symbol_in = matches[1]
            if symbol_in != symbol:
                raise DataParserError(kwargs['filename'], kwargs.get('worksheet'))
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                     data_row.timestamp,
                                                     buy_quantity=quantity,
                                                     buy_asset=symbol,
                                                     wallet=WALLET)
        else:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                     data_row.timestamp,
                                                     buy_quantity=row_dict['Amount'],
                                                     buy_asset=symbol,
                                                     wallet=WALLET)
    elif row_dict['Type'] == "SENT":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['Amount'],
                                                 sell_asset=symbol,
                                                 fee_quantity=row_dict['Fee'],
                                                 fee_asset=fee_symbol,
                                                 wallet=WALLET)
    elif row_dict['Type'] == "SELF":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=0,
                                                 sell_asset=symbol,
                                                 fee_quantity=row_dict['Fee'],
                                                 fee_asset=fee_symbol,
                                                 wallet=WALLET)
    elif row_dict['Type'] == "FAILED":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                 data_row.timestamp,
                                                 sell_quantity=0,
                                                 sell_asset=symbol,
                                                 fee_quantity=row_dict['Fee'],
                                                 fee_asset=fee_symbol,
                                                 wallet=WALLET,
                                                 note="Failure")
    else:
        raise UnexpectedTypeError(parser.in_header.index('Type'), 'Type', row_dict['Type'])


def trezor_suite_fiat_header_check(col_name):
    return re.search("Fiat \\(.*\\)", col_name) is not None


DataParser(DataParser.TYPE_WALLET,
           "Trezor Suite",
           ['Timestamp', 'Date', 'Time', 'Type', 'Transaction ID', 'Fee', 'Fee unit', 'Address', 'Label', 'Amount',
            'Amount unit', trezor_suite_fiat_header_check, 'Other'],
           worksheet_name="Trezor",
           row_handler=parse_trezor_suite_new)
