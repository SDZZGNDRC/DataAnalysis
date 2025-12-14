# hftbacktest_okx

输入: 指定的books文件和trades文件，均为7z格式，且每个7z文件中只包含一个json文件。

处理流程:
1. 解压每一个7z文件并读取其中的json文件到内存中
2. 解析为np数组，数组的每一列的字段必须符合hftbacktest的输入要求
3. 将books数组和trades数组合并，合并流程由`merge_event_arrays_by_exch_ts`实现
```python

@njit
def merge_arrays_by_exch_ts(depth_data: EVENT_ARRAY, trade_data: EVENT_ARRAY) -> EVENT_ARRAY:
    """
    基于交易所时间戳归并两个已排序的事件数组（njit优化版本）。
    
    Args:
        depth_data: 深度事件数组（按本地接收顺序）
        trade_data: 交易事件数组（按本地接收顺序）
    
    Returns:
        归并后的事件数组
    """
    len_d = len(depth_data)
    len_t = len(trade_data)
    
    if len_d == 0:
        return trade_data
    if len_t == 0:
        return depth_data
    
    # 创建结果数组
    print(f'[debug] Merging {len_d} depth events and {len_t} trade events by exch_ts')
    merged = np.empty(len_d + len_t, depth_data.dtype)
    print(f'[debug] Created merged array of size {len(merged)}')
    
    # 归并过程
    ptr_d = 0
    ptr_t = 0
    idx = 0
    
    while ptr_d < len_d and ptr_t < len_t:
        if depth_data[ptr_d].exch_ts <= trade_data[ptr_t].exch_ts:
            merged[idx] = depth_data[ptr_d]
            ptr_d += 1
        else:
            merged[idx] = trade_data[ptr_t]
            ptr_t += 1
        idx += 1
    
    # 处理剩余的深度事件
    while ptr_d < len_d:
        merged[idx] = depth_data[ptr_d]
        ptr_d += 1
        idx += 1
    
    # 处理剩余的交易事件
    while ptr_t < len_t:
        merged[idx] = trade_data[ptr_t]
        ptr_t += 1
        idx += 1
    
    return merged
```

4. 对得到的最终数组进行修正以及校验
5. 生成多个npz文件，这些npz文件就是直接可以输入给hftbacktest的

流程1和2的目的就是将7z格式的数据转化为符合hftbacktest输入要求的np数组，这两步可以使用多进程处理，
但是需要注意的是，每个7z文件生成的np数组必须直接保存到磁盘中，而不是直接返回给主进程，保存格式为memmap。

