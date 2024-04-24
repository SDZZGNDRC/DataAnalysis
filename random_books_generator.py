from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys
from scipy import signal
from pathlib import Path


def books_generator(path: str, books: Dict[int, List]) -> None:
    # TODO: Need support to repeated book-levels
    columns = ['price', 'size', 'numOrders', 'side', 'action', 'timestamp', 'instId']
    tmp_books = sorted(books.items(), key=lambda x:x[0])
    new_books: Dict[int, List] = {}
    for i in range(1, len(tmp_books)):
        px_with_side = list(map(lambda x: (x[0], x[3]), tmp_books[i][1]))
        ts = tmp_books[i][0]
        clear_bls = []
        for last_bl in tmp_books[i-1][1]:
            # TODO: Maybe can use binary search here?
            if (last_bl[0], last_bl[3]) not in px_with_side:    # remove this book level
                updated_bl = last_bl.copy()
                updated_bl[1] = 0
                updated_bl[4] = 'update'
                updated_bl[-2] = ts
                clear_bls.append(updated_bl)
            else:
                continue
        new_books[ts] = clear_bls + tmp_books[i][1]
    new_books[0] = tmp_books[0][1]
    
    df = pd.DataFrame(sorted([bl for bls in new_books.values() for bl in bls], key=lambda x: x[-2]), columns=columns)
    df.to_parquet(path, compression='gzip', index=False)


def random_books_generator( px: List[float], 
                            tss: List[int], 
                            path: str, 
                            instId: str = 'TEST-USDT',
                            size: float = 1.0,
                            len_bls: int = 5) -> None:
    if len(px) != len(tss):
        raise ValueError('Length of px and tss should be the same')
    px = [float(x) for x in px]
    tss = sorted([int(x) for x in tss])
    size = float(size)
    
    books: Dict[int, List] = {}
    # price: float, size: float, numOrders: int, side: str, action: str, timestamp: int, instId: str
    books[tss[0]] = list(map(lambda x: [float(px[0]+1*x), size, 1, 'ask' if x>=0 else 'bid', 'snapshot', tss[0], instId], [i for i in range(1, len_bls+1)]+[-1*i for i in range(1, len_bls+1)]))
    for t, ts in enumerate(tss[1:]):
        books[ts] = list(map(lambda x: [float(px[1:][t]+1*x), size, 1, 'ask' if x>=0 else 'bid', 'update', ts, instId], [i for i in range(1, len_bls+1)]+[-1*i for i in range(1, len_bls+1)]))

    books_generator(path, books)


def BG_sine(path: str) -> None:
    x = np.arange(0, 2*np.pi, 0.01)
    y = 1000+np.sin(x)*100

    px = [round(_, 1) for _ in y]
    tss = [ _ for _ in range(0, len(x)*1000, 1000) ]
    path = Path(path) / 'SINE-USDT'
    if not path.exists():
        path.mkdir()
    random_books_generator(
        px, tss, 
        path/'part-0-0-628000.parquet',
        instId='SINE-USDT',
        )
    # Plot the sine function
    plt.plot(tss, px)
    plt.xlabel('tss')
    plt.ylabel('px')
    plt.title('Sine Function')
    plt.grid(True)
    # dump as a png picture
    plt.savefig('Sine.png')

def BG_triangle(path: str, sample_points: int = 1000, cycles: int = 3) -> None:
    t = np.linspace(0, 1, sample_points)
    px = (1000 + 100*signal.sawtooth(2 * np.pi * cycles * t, 0.5)).round(1)
    tss = [ _ for _ in range(0, len(t)*1000, 1000) ]
    path = Path(path) / 'TRIANGLE-USDT'
    if not path.exists():
        path.mkdir()
    random_books_generator(
        px, tss, 
        path / rf'part-0-0-{tss[-1]}.parquet',
        instId='TRIANGLE-USDT'
        )
    # Plot the triangle function
    plt.plot(tss, px)
    plt.xlabel('tss')
    plt.ylabel('px')
    plt.title('Triangle Function')
    plt.grid(True)
    # dump as a png picture
    plt.savefig('Triangle.png')


def BG_case1(path: str) -> None:
    books: Dict[int, List] = {}
    # price: float, size: float, numOrders: int, side: str, action: str, timestamp: int, instId: str
    
    ts = 0
    books[ts] = [
        [100.0, 0.5, 2, 'ask', 'snapshot', ts, 'CASE1-USDT'],
        [101.0, 0.4, 2, 'ask', 'snapshot', ts, 'CASE1-USDT'],
        [102.0, 0.3, 2, 'ask', 'snapshot', ts, 'CASE1-USDT'],
        [103.0, 0.2, 2, 'ask', 'snapshot', ts, 'CASE1-USDT'],
        [104.0, 0.1, 2, 'ask', 'snapshot', ts, 'CASE1-USDT'],
        [ 99.0, 0.5, 2, 'bid', 'snapshot', ts, 'CASE1-USDT'],
        [ 98.0, 0.4, 2, 'bid', 'snapshot', ts, 'CASE1-USDT'],
        [ 97.0, 0.3, 2, 'bid', 'snapshot', ts, 'CASE1-USDT'],
        [ 96.0, 0.2, 2, 'bid', 'snapshot', ts, 'CASE1-USDT'],
        [ 95.0, 0.1, 2, 'bid', 'snapshot', ts, 'CASE1-USDT'],
    ]
    
    ts = 1000
    books[ts] = [
        [100.0, 0.1, 2, 'ask', 'update', ts, 'CASE1-USDT'],   # changed
        [101.0, 1.4, 2, 'ask', 'update', ts, 'CASE1-USDT'],   # changed
        [102.0, 0.3, 1, 'ask', 'update', ts, 'CASE1-USDT'],   # changed
        [103.0, 0.7, 3, 'ask', 'update', ts, 'CASE1-USDT'],   # changed
        [104.0, 0.5, 2, 'ask', 'update', ts, 'CASE1-USDT'],   # changed
        [ 99.0, 0.5, 2, 'bid', 'update', ts, 'CASE1-USDT'],   
        [ 98.0, 0.4, 2, 'bid', 'update', ts, 'CASE1-USDT'],   
        [ 97.0, 0.3, 2, 'bid', 'update', ts, 'CASE1-USDT'],
        [ 96.0, 0.2, 2, 'bid', 'update', ts, 'CASE1-USDT'],
        [ 95.0, 0.1, 2, 'bid', 'update', ts, 'CASE1-USDT'],
    ]
    
    ts = 2000
    books[ts] = [
        [100.0, 0.1, 2, 'ask', 'update', ts, 'CASE1-USDT'],
        [101.0, 1.4, 2, 'ask', 'update', ts, 'CASE1-USDT'],
        [102.0, 0.3, 1, 'ask', 'update', ts, 'CASE1-USDT'],
        [103.0, 0.7, 3, 'ask', 'update', ts, 'CASE1-USDT'],
        [104.0, 0.5, 2, 'ask', 'update', ts, 'CASE1-USDT'],
        [ 99.0, 1.5, 1, 'bid', 'update', ts, 'CASE1-USDT'],   # changed   
        [ 98.0, 3.4, 2, 'bid', 'update', ts, 'CASE1-USDT'],   # changed   
        [ 97.0, 2.3, 4, 'bid', 'update', ts, 'CASE1-USDT'],   # changed
        [ 96.0, 1.2, 3, 'bid', 'update', ts, 'CASE1-USDT'],   # changed
        [ 95.0, 0.3, 1, 'bid', 'update', ts, 'CASE1-USDT'],   # changed
    ]
    t_path = Path(path) / 'CASE1-USDT'
    t_path.mkdir(exist_ok=True)
    books_generator(t_path / f'part-0-0-{ts}.parquet', books)


def BG_case2(path: str) -> None:
    instId = 'CASE2-USDT'
    books: Dict[int, List] = {}
    # price: float, size: float, numOrders: int, side: str, action: str, timestamp: int, instId: str
    
    ts = 0
    books[ts] = [
        [100.0, 0.5, 2, 'ask', 'snapshot', ts, instId],
        [101.0, 0.4, 2, 'ask', 'snapshot', ts, instId],
        [102.0, 0.3, 2, 'ask', 'snapshot', ts, instId],
        [103.0, 0.2, 2, 'ask', 'snapshot', ts, instId],
        [104.0, 0.1, 2, 'ask', 'snapshot', ts, instId],
        [ 99.0, 0.5, 2, 'bid', 'snapshot', ts, instId],
        [ 98.0, 0.4, 2, 'bid', 'snapshot', ts, instId],
        [ 97.0, 0.3, 2, 'bid', 'snapshot', ts, instId],
        [ 96.0, 0.2, 2, 'bid', 'snapshot', ts, instId],
        [ 95.0, 0.1, 2, 'bid', 'snapshot', ts, instId],
    ]
    
    ts = 1000    # blcsi = 11
    books[ts] = [
        [100.0, 0.1, 2, 'ask', 'update', ts, instId],   # changed
        [101.0, 1.4, 2, 'ask', 'update', ts, instId],   # changed
        [102.0, 0.3, 1, 'ask', 'update', ts, instId],   # changed
        [103.0, 0.7, 3, 'ask', 'update', ts, instId],   # changed
        [104.0, 0.5, 2, 'ask', 'update', ts, instId],   # changed
        [105.0, 1.1, 1, 'ask', 'update', ts, instId],   # changed
        # [ 99.0, 0.0, 2, 'bid', 'update', 1000, instId],   # changed
        [ 98.0, 0.4, 2, 'bid', 'update', ts, instId],   # changed
        [ 97.0, 0.3, 2, 'bid', 'update', ts, instId],   # changed
        [ 96.0, 0.2, 2, 'bid', 'update', ts, instId],   # changed
        [ 95.0, 0.1, 2, 'bid', 'update', ts, instId],   # changed
    ]
    
    ts = 2000    # blcsi = 6
    books[ts] = [
        [100.0, 0.1, 2, 'ask', 'update', ts, instId],   
        [101.0, 1.4, 2, 'ask', 'update', ts, instId],   
        [102.0, 0.3, 1, 'ask', 'update', ts, instId],   
        [103.0, 0.7, 3, 'ask', 'update', ts, instId],   
        [104.0, 0.5, 2, 'ask', 'update', ts, instId],   
        # [105.0, 0.0, 1, 'ask', 'update', ts, instId],   # changed
        [ 99.0, 0.2, 1, 'bid', 'update', ts, instId],   # changed
        [ 98.0, 0.4, 2, 'bid', 'update', ts, instId],   # changed
        [ 97.0, 0.3, 2, 'bid', 'update', ts, instId],   # changed
        [ 96.0, 0.2, 2, 'bid', 'update', ts, instId],   # changed
        [ 95.0, 0.1, 2, 'bid', 'update', ts, instId],   # changed
    ]
    t_path = Path(path) / instId
    t_path.mkdir(exist_ok=True)
    books_generator(t_path / f'part-0-0-{ts}.parquet', books)


def BG_case3(path: str) -> None:
    instId = 'CASE3-USDT'
    books: Dict[int, List] = {}
    # price: float, size: float, numOrders: int, side: str, action: str, timestamp: int, instId: str
    
    ts = 0
    books[ts] = [
        [100.0, 0.5, 2, 'ask', 'snapshot', ts, instId],
        [101.0, 0.4, 2, 'ask', 'snapshot', ts, instId],
        [102.0, 0.3, 2, 'ask', 'snapshot', ts, instId],
        [103.0, 0.2, 2, 'ask', 'snapshot', ts, instId],
        [104.0, 0.1, 2, 'ask', 'snapshot', ts, instId],
        [ 99.0, 0.5, 2, 'bid', 'snapshot', ts, instId],
        [ 98.0, 0.4, 2, 'bid', 'snapshot', ts, instId],
        [ 97.0, 0.3, 2, 'bid', 'snapshot', ts, instId],
        [ 96.0, 0.2, 2, 'bid', 'snapshot', ts, instId],
        [ 95.0, 0.1, 2, 'bid', 'snapshot', ts, instId],
    ]
    
    ts = 1000    # blcsi = 11
    books[ts] = [
        [100.0, 0.1, 2, 'ask', 'update', ts, instId],   # changed
        [101.0, 1.4, 2, 'ask', 'update', ts, instId],   # changed
        [102.0, 0.3, 1, 'ask', 'update', ts, instId],   # changed
        [103.0, 0.7, 3, 'ask', 'update', ts, instId],   # changed
        [104.0, 0.5, 2, 'ask', 'update', ts, instId],   # changed
        [105.0, 1.1, 1, 'ask', 'update', ts, instId],   # changed
        # [ 99.0, 0.0, 2, 'bid', 'update', 1000, instId],   # changed
        [ 98.0, 0.4, 2, 'bid', 'update', ts, instId],   # changed
        [ 97.0, 0.3, 2, 'bid', 'update', ts, instId],   # changed
        [ 96.0, 0.2, 2, 'bid', 'update', ts, instId],   # changed
        [ 95.0, 0.1, 2, 'bid', 'update', ts, instId],   # changed
    ]
    
    ts = 2000    # blcsi = 6
    books[ts] = [
        [100.0, 0.1, 2, 'ask', 'update', ts, instId],   
        [101.0, 1.4, 2, 'ask', 'update', ts, instId],   
        [102.0, 0.3, 1, 'ask', 'update', ts, instId],   
        [103.0, 0.7, 3, 'ask', 'update', ts, instId],   
        [104.0, 0.5, 2, 'ask', 'update', ts, instId],   
        # [105.0, 0.0, 1, 'ask', 'update', ts, instId],   # changed
        [ 99.0, 0.2, 1, 'bid', 'update', ts, instId],   # changed
        [ 98.0, 0.4, 2, 'bid', 'update', ts, instId],   # changed
        [ 97.0, 0.3, 2, 'bid', 'update', ts, instId],   # changed
        [ 96.0, 0.2, 2, 'bid', 'update', ts, instId],   # changed
        [ 95.0, 0.1, 2, 'bid', 'update', ts, instId],   # changed
    ]
    t_path = Path(path) / instId
    t_path.mkdir(exist_ok=True)
    books_generator(t_path / f'part-0-0-{ts}.parquet', books)
    books = {}
    
    ts = 3000
    books[ts] = [
        [100.0, 0.1, 2, 'ask', 'snapshot', ts, instId],   
        [101.0, 1.4, 2, 'ask', 'snapshot', ts, instId],   
        [102.0, 0.3, 1, 'ask', 'snapshot', ts, instId],   
        [103.0, 0.7, 3, 'ask', 'snapshot', ts, instId],   
        [104.0, 0.5, 2, 'ask', 'snapshot', ts, instId],   
        [ 99.0, 0.2, 1, 'bid', 'snapshot', ts, instId],   
        [ 98.0, 0.4, 2, 'bid', 'snapshot', ts, instId],   
        [ 97.0, 0.3, 2, 'bid', 'snapshot', ts, instId],   
        [ 96.0, 0.2, 2, 'bid', 'snapshot', ts, instId],   
        [ 95.0, 0.1, 2, 'bid', 'snapshot', ts, instId],   
    ]
    
    ts = 4000     # blcsi = 7
    books[ts] = [
        [100.0, 0.1, 1, 'ask', 'update', ts, instId],   # changed
        [101.0, 0.6, 2, 'ask', 'update', ts, instId],   # changed
        [102.0, 0.3, 1, 'ask', 'update', ts, instId],   
        # [103.0, 0.7, 3, 'ask', 'update', ts, instId],   
        [104.0, 0.5, 2, 'ask', 'update', ts, instId],   # changed
        [105.0, 0.4, 2, 'ask', 'update', ts, instId],   # changed
        [ 99.5, 0.1, 1, 'bid', 'update', ts, instId],   # changed
        [ 99.0, 0.2, 1, 'bid', 'update', ts, instId],   # changed
        # [ 98.0, 0.4, 2, 'bid', 'update', ts, instId],   
        [ 97.0, 0.3, 2, 'bid', 'update', ts, instId],   
        [ 96.0, 0.2, 2, 'bid', 'update', ts, instId],   
        [ 95.0, 1.3, 1, 'bid', 'update', ts, instId],   # changed
    ]
    
    
    books_generator(t_path / f'part-1-{3000}-{ts}.parquet', books)



if __name__ == '__main__':
    options = {
        'sine': BG_sine,
        'triangle': BG_triangle,
        'case1': BG_case1,
        'case2': BG_case2,
        'case3': BG_case3,
    }
    if len(sys.argv)!= 3:
        print('Usage: python3 books_generator.py <book_type> <path>')
        print(f'book type: {", ".join(options.keys())}')
        exit(-1)
    
    if sys.argv[1] in options:
        options[sys.argv[1]](sys.argv[2])
    else:
        print(f'Unknown options: {sys.argv[1]}')
        exit(-1)




