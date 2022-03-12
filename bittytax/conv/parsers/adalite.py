import decimal

from bittytax.conv.out_record import TransactionOutRecord
from bittytax.conv.dataparser import DataParser
from bittytax.conv.exceptions import UnexpectedTypeError

WALLET = "ADALITE"

def parse_adalite(data_row, parser, **_kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['Date'])
    if row_dict['Type'] == "Received":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Received amount'],
                                                 buy_asset=row_dict['Received currency'],
                                                 wallet=WALLET,
                                                 note=row_dict['Transaction ID'])
    elif row_dict['Type'] == "Sent":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=abs(decimal.Decimal(row_dict['Sent amount'])),
                                                 sell_asset=row_dict['Sent currency'],
                                                 fee_quantity=row_dict['Fee amount'],
                                                 fee_asset=row_dict['Fee currency'],
                                                 wallet=WALLET,
                                                 note=row_dict['Transaction ID'])
    elif row_dict['Type'] == "Reward awarded":
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_STAKING,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['Received amount'],
                                                 buy_asset=row_dict['Received currency'],
                                                 wallet=WALLET,
                                                 note=row_dict['Transaction ID'])
    else:
        raise UnexpectedTypeError(parser.in_header.index('Type'), 'Type', row_dict['Type'])


DataParser(DataParser.TYPE_WALLET,
           "ADALITE",
           ['Date', 'Transaction ID', 'Type', 'Received amount', 'Received currency', 'Sent amount', 'Sent currency', 'Fee amount', 'Fee currency', ''],
           worksheet_name="ADALITE",
           row_handler=parse_adalite)
