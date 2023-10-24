import glob
import os
from typing import Dict
import subprocess
from multiprocessing import Pool
from tqdm.contrib.concurrent import process_map

dataSource = 'E:\\datapool'
tempDir = 'E:\\temp'

archived_data: Dict[str, str] = {}

for root, dirs, files in os.walk(dataSource):
    for file in files:
        if file.endswith('.7z'):
            archived_data[file] = os.path.join(root, file)


# def unzip_7z(file_info):
#     _, filePath = file_info
#     command = 'D:\\Software\\7-Zip\\7z.exe e ' + filePath + ' -o' + tempDir
#     subprocess.run(command, shell=True)

def unzip_7z(args):
    zipPath, destPath = args
    command = 'D:\\Software\\7-Zip\\7z.exe e ' + zipPath + ' -o' + destPath
    subprocess.run(command, shell=True)



if __name__ == "__main__":
    destRoot = f'E:\\temp\\json\\BTC-USDT-FUTURES'
    os.makedirs(destRoot, exist_ok=True)
    zipfiles = glob.glob(f'E:\\datapool\\2023-08-*\\OKX-Books-BTC-USDT-2[3-4]*-400-*.7z')
    assert zipfiles
    destfiles = [destRoot]*len(zipfiles)
    # for zipfile in zipfiles:
            # filename = os.path.splitext(os.path.basename(zipfile))[0] + '.json'
            # destfiles.append(os.path.join(destRoot, filename))
            # destfiles.append(destRoot)

    print('Start to unzip...')
    # r = process_map(unzip_7z, zip(zipfiles, destfiles), max_workers=6)

    with Pool(6) as p:
        p.map(unzip_7z, zip(zipfiles, destfiles))
