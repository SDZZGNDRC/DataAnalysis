from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys
from scipy import signal
from pathlib import Path

def random_books_generator( px: List[float], 
                            tss: List[int], 
                            path: str, 
                            instId: str = 'TEST-USDT',
                            size: float = 1.0,
                            len_bls: int = 5) -> None:
    if len(px) != len(tss):
        raise ValueError('Length of px and tss should be the same')
    columns = ['price', 'size', 'numOrders', 'side', 'action', 'timestamp', 'instId']
    px = [float(x) for x in px]
    tss = sorted([int(x) for x in tss])
    size = float(size)
    
    books: Dict[int, List] = {}
    books[tss[0]] = list(map(lambda x: [float(px[0]+1*x), size, 1, 'ask' if x>=0 else 'bid', 'snapshot', tss[0], instId], [i for i in range(1, len_bls+1)]+[-1*i for i in range(1, len_bls+1)]))
    for t, ts in enumerate(tss[1:]):
        books[ts] = list(map(lambda x: [float(px[1:][t]+1*x), size, 1, 'ask' if x>=0 else 'bid', 'update', ts, instId], [i for i in range(1, len_bls+1)]+[-1*i for i in range(1, len_bls+1)]))

    tmp_books = sorted(books.items(), key=lambda x:x[0])
    new_books: Dict[int, List] = {}
    for i in range(1, len(tmp_books)):
        px_with_side = list(map(lambda x: (x[0], x[3]), tmp_books[i][1]))
        ts = tmp_books[i][0]
        clear_bls = []
        for last_bl in tmp_books[i-1][1]:
            if (last_bl[0], last_bl[3]) not in px_with_side:
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


def sine_books(path: str) -> None:
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

def triangle_books(path: str, sample_points: int = 1000, cycles: int = 3) -> None:
    t = np.linspace(0, 1, sample_points)
    px = (1000 + 100*signal.sawtooth(2 * np.pi * cycles * t, 0.5)).round(1)
    tss = [ _ for _ in range(0, len(t)*1000, 1000) ]
    path = Path(path) / 'TRIANGLE-USDT'
    if not path.exists():
        path.mkdir()
    random_books_generator(
        px, tss, 
        path/rf'part-0-0-{tss[-1]}.parquet',
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


if __name__ == '__main__':
    if len(sys.argv)!= 3:
        print('Usage: python3 books_generator.py <book_type> <path>')
        print('book type: sine, triangle')
        exit(-1)
    if sys.argv[1] =='sine':
        sine_books(sys.argv[2])
    elif sys.argv[1] == 'triangle':
        triangle_books(sys.argv[2])
    else:
        print(f'Unknown book type: {sys.argv[1]}')
        exit(-1)




