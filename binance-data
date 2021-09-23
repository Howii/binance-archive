#!/usr/bin/env python
from util import BinanceArchive, get_parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    params = {
        "trading_type": args.trading_type,
        "mkt_data_type": args.mkt_data_type,
        "interval": args.interval,
        "start_date": args.start_date,
        "end_date": args.end_date
    }

    for symbol in args.symbols:
        params["symbol"] = symbol
        ba = BinanceArchive.from_params(params)
        ba.download()


if __name__ == '__main__':
    main()
