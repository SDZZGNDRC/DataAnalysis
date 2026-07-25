import dirtyjson

file_path = r"C:\Users\SDZZ\Downloads\OKX-Trades-BTC-USDT-1745898736181-1745915946208.json"

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        # 首先读取整个文件内容为字符串
        raw_str = f.read()
        # dirtyjson 使用 loads 方法解析字符串
        data = dirtyjson.loads(raw_str)
        print("使用 dirtyjson 解析成功！")
        # 你可以打印一部分数据来验证
        # print(data)
except Exception as e:
    print(f"使用 dirtyjson 解析时发生错误: {e}")