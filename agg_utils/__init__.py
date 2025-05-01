from . import Books, pattern1, pattern2


category_map = {
    'Books': Books.map,
    'EstimatedPrice': pattern1.map,
    'FundingRate': pattern1.map,
    'IndexTickers': pattern1.map,
    'LiquidationOrders': pattern2.map,
    'MarkPrice': pattern1.map,
    'OpenInterest': pattern1.map,
    'OptDeal': pattern1.map,
    'OptSummary': pattern1.map,
    'PriceLimit': pattern1.map,
    'Status': pattern1.map,
    'Tickers': pattern1.map,
    'Trades': pattern1.map,
}

def get_category_map(category: str):
    if category in category_map:
        return category_map[category]
    else:
        raise NotImplementedError
