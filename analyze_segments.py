import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import sys

def analyze_segments(input_file, output_file):
    """
    分析segments数据，计算持续时间和相邻间隔
    """
    # 读取CSV文件
    df = pd.read_csv(input_file)
    
    # 过滤prevSeqId = -1的segments
    filtered_df = df[df['prevSeqId'] == -1].copy()
    
    # 计算每个segment的持续时间（小时单位）
    # 时间戳是毫秒，转换为小时： (end_timestamp - start_timestamp) / (1000 * 60 * 60)
    filtered_df['duration_hours'] = (filtered_df['end_timestamp'] - filtered_df['start_timestamp']) / (1000 * 60 * 60)
    
    # 按start_timestamp排序
    filtered_df = filtered_df.sort_values('start_timestamp').reset_index(drop=True)
    
    # 计算相邻segments的时间间隔（秒单位）
    # 下一个segment的start_timestamp - 当前segment的end_timestamp，然后转换为秒
    filtered_df['gap_to_next_seconds'] = np.nan
    
    for i in range(len(filtered_df) - 1):
        current_end = filtered_df.loc[i, 'end_timestamp']
        next_start = filtered_df.loc[i + 1, 'start_timestamp']
        gap_seconds = (next_start - current_end) / 1000  # 转换为秒
        filtered_df.loc[i, 'gap_to_next_seconds'] = gap_seconds
    
    # 选择要输出的列
    output_columns = [
        'prevSeqId', 'seqId', 'start_file', 'end_file',
        'start_timestamp', 'start_datetime', 'end_timestamp', 'end_datetime',
        'duration_hours', 'gap_to_next_seconds'
    ]
    
    # 输出到新的CSV文件
    filtered_df[output_columns].to_csv(output_file, index=False)
    
    # 打印统计信息
    print(f"分析完成！共处理 {len(filtered_df)} 个prevSeqId=-1的segments")
    print(f"持续时间统计（小时）：")
    print(f"  最小值: {filtered_df['duration_hours'].min():.2f}")
    print(f"  最大值: {filtered_df['duration_hours'].max():.2f}")
    print(f"  平均值: {filtered_df['duration_hours'].mean():.2f}")
    print(f"  中位数: {filtered_df['duration_hours'].median():.2f}")
    
    # 计算间隔统计（排除NaN值）
    gaps = filtered_df['gap_to_next_seconds'].dropna()
    if len(gaps) > 0:
        print(f"\n相邻间隔统计（秒）：")
        print(f"  最小值: {gaps.min():.2f}")
        print(f"  最大值: {gaps.max():.2f}")
        print(f"  平均值: {gaps.mean():.2f}")
        print(f"  中位数: {gaps.median():.2f}")
        
        # 统计间隔分布（秒单位）
        print(f"\n间隔分布：")
        print(f"  < 1秒: {len(gaps[gaps < 1])}")
        print(f"  1-10秒: {len(gaps[(gaps >= 1) & (gaps < 10)])}")
        print(f"  10-60秒: {len(gaps[(gaps >= 10) & (gaps < 60)])}")
        print(f"  1-10分钟: {len(gaps[(gaps >= 60) & (gaps < 600)])}")
        print(f"  10-60分钟: {len(gaps[(gaps >= 600) & (gaps < 3600)])}")
        print(f"  > 1小时: {len(gaps[gaps >= 3600])}")
    
    return filtered_df

def main():
    """主函数，处理命令行参数并执行分析"""
    parser = argparse.ArgumentParser(description='分析segments数据，计算持续时间和相邻间隔')
    parser.add_argument('input_file', help='输入的CSV文件路径')
    parser.add_argument('-o', '--output', default='segments_analysis.csv',
                       help='输出的CSV文件路径 (默认: segments_analysis.csv)')
    
    args = parser.parse_args()
    
    print(f"开始分析segments数据...")
    print(f"输入文件: {args.input_file}")
    print(f"输出文件: {args.output}")
    
    try:
        result_df = analyze_segments(args.input_file, args.output)
        print(f"结果已保存到: {args.output}")
    except FileNotFoundError:
        print(f"错误: 找不到输入文件 '{args.input_file}'")
        sys.exit(1)
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()