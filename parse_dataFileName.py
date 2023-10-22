
import os

def parse_dataFileName(fileBaseName: str) -> dict:
    # example: OKX-Books-1INCH-USD-SWAP-400-1689297329268-1689298999939.7z
    items = os.path.splitext(fileBaseName)[0].split('-')
    if len(items) < 6:
        raise Exception('Invalid file name: ' + fileBaseName)
    
    exchange = items[0]
    dataSource = items[1]
    startTimestamp = items[-2]
    endTimestamp = items[-1]
    dsID = '-'.join(items[2:-2])
    
    return {
        'exchange': exchange,
        'dataSource': dataSource,
        'dsID': dsID,
        'startTimestamp': startTimestamp,
        'endTimestamp': endTimestamp,
    }