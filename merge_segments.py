import argparse
import csv
from datetime import datetime
import sys

def parse_duration(duration_str):
    if not duration_str:
        raise ValueError("Empty duration string")
    
    unit = duration_str[-1].lower()
    try:
        value = int(duration_str[:-1])
    except ValueError:
        raise ValueError(f"Invalid duration format: {duration_str}")
        
    if unit == 's':
        return value * 1000
    elif unit == 'm':
        return value * 60000
    elif unit == 'h':
        return value * 3600000
    elif unit == 'd':
        return value * 86400000
    else:
        raise ValueError(f"Unknown time unit: {unit}. Supported: s, m, h, d")

def timestamp_to_datetime(timestamp):
    dt = datetime.fromtimestamp(timestamp / 1000)
    return dt.strftime('%Y-%m-%d %H:%M')

def main():
    parser = argparse.ArgumentParser(description="Merge segments based on time gap.")
    parser.add_argument('--input', default='reconstructed_segments.csv', help='Input CSV file')
    parser.add_argument('--output', default='merged_segments.csv', help='Output CSV file')
    parser.add_argument('--max-gap', required=True, help='Max gap e.g. 1m, 1h')
    parser.add_argument('--max-duration', help='Max duration e.g. 1h, 1d')
    
    args = parser.parse_args()
    
    try:
        max_gap_ms = parse_duration(args.max_gap)
    except ValueError as e:
        print(f"Error parsing max-gap: {e}")
        sys.exit(1)

    max_duration_ms = None
    if args.max_duration:
        try:
            max_duration_ms = parse_duration(args.max_duration)
            print(f"Max duration: {max_duration_ms} ms")
        except ValueError as e:
            print(f"Error parsing max-duration: {e}")
            sys.exit(1)
        
    print(f"Max gap: {max_gap_ms} ms")

    segments = []
    try:
        with open(args.input, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['prevSeqId'] == '-1':
                    # Parse timestamps
                    try:
                        row['start_timestamp'] = int(row['start_timestamp'])
                        row['end_timestamp'] = int(row['end_timestamp'])
                        segments.append(row)
                    except ValueError:
                        print(f"Skipping invalid row: {row}")
    except FileNotFoundError:
        print(f"Input file not found: {args.input}")
        sys.exit(1)

    if not segments:
        print("No segments with prevSeqId == -1 found.")
        return

    # Sort by start_timestamp ascending
    segments.sort(key=lambda x: x['start_timestamp'])
    
    final_segments = []
    final_segments.append(segments[0])
    
    merged_count = 0
    
    for i in range(1, len(segments)):
        current = segments[i]
        best_match_idx = -1
        min_diff = float('inf')
        
        for j, candidate in enumerate(final_segments):
            # Calculate gap: current start - candidate end
            diff = current['start_timestamp'] - candidate['end_timestamp']
            
            if diff < 0:
                print(f'Error: diff({diff}) < 0')
                exit(-1)
            
            # We only merge if diff is within threshold.
            # diff <= max_gap_ms.
            # We allow overlaps (diff < 0) as they are "close".
            if diff <= max_gap_ms:
                # Check max duration
                new_end = max(candidate['end_timestamp'], current['end_timestamp'])
                new_duration = new_end - candidate['start_timestamp']

                if max_duration_ms is None or new_duration <= max_duration_ms:
                    if diff < min_diff:
                        min_diff = diff
                        best_match_idx = j
        
        if best_match_idx != -1:
            # Merge
            target = final_segments[best_match_idx]
            
            # If current ends after target, update target end
            if current['end_timestamp'] > target['end_timestamp']:
                target['end_timestamp'] = current['end_timestamp']
                target['end_datetime'] = timestamp_to_datetime(target['end_timestamp'])
            
            # Append covered files
            if current['covered_files']:
                if target['covered_files']:
                    target['covered_files'] += ';' + current['covered_files']
                else:
                    target['covered_files'] = current['covered_files']
            
            # Update duration
            duration = (target['end_timestamp'] - target['start_timestamp']) / 3600000.0
            target['duration_hours'] = f"{duration:.4f}"
            
            merged_count += 1
        else:
            final_segments.append(current)

    print(f"Processed {len(segments)} segments.")
    print(f"Merged {merged_count} times.")
    print(f"Resulting in {len(final_segments)} final segments.")
    
    # Write output
    fieldnames = ['prevSeqId', 'seqId', 'startSeqId', 'start_timestamp', 'start_datetime',
                  'end_timestamp', 'end_datetime', 'duration_hours', 'covered_files']
    
    with open(args.output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final_segments)
        
    print(f"Written to {args.output}")

if __name__ == "__main__":
    main()