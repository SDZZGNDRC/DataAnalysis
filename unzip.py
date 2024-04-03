import glob
import os
import sys
from multiprocessing import Pool
import py7zr

def unzip_7z(args):
    zipPath, destPath = args
    with py7zr.SevenZipFile(zipPath, mode='r') as z:
        z.extractall(destPath)



if __name__ == "__main__":
    if len(sys.argv[1:]) != 2:
        print('Please input two argv')
        exit(-1)
    zipDir, destDir = sys.argv[1], sys.argv[2]
    if not (os.path.isdir(zipDir) and os.path.isdir(destDir)):
        print('Please input directories')
        exit(-1)
    if not os.path.exists(zipDir):
        print(f'zipDir {zipDir} not existed')
        exit(-1)

    os.makedirs(destDir, exist_ok=True)
    zipfiles = glob.glob(os.path.join(zipDir, 'OKX-*.7z'))
    if not zipfiles:
        print(f'Can not find any zip files under: {zipDir}')
        exit(-1)
    destfiles = [destDir]*len(zipfiles)

    print('Start to unzip...')

    cpu_nums = os.cpu_count()
    with Pool(cpu_nums) as p:
        p.map(unzip_7z, zip(zipfiles, destfiles))
