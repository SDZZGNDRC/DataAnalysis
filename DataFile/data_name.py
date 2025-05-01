class DataName:
    # example: OKX-Books-1INCH-USD-SWAP-400-1689297329268-1689298999939.7z
    @staticmethod
    def validate_name(name: str) -> bool:
        # endswith `.7z` or `.json` or `.parquet`
        if not (name.endswith('.7z') or name.endswith('.json') or name.endswith('.parquet')):
            return False
        
        # split by '-'
        splitted_name = name.split('-')
        
        # component 0: exchange, only `OKX`
        if splitted_name[0] != 'OKX':
            return False
        
        # component 1: category
        valid_categories = [
            'Books', 'EstimatedPrice', 'FundingRate', 'IndexTickers', 'LiquidationOrders', 
            'MarkPrice', 'OpenInterest', 'Tickers', 'Trades'
        ]
        
        if splitted_name[1] not in valid_categories:
            return False
        
        # component -2 and -1 are start and end unix timestamps: 
        if not (splitted_name[-2].isdigit() and splitted_name[-1].split(".")[0].isdigit()):
            return False
        
        # start timestamp must not bigger than end timestamp
        start_ts = splitted_name[-2]
        end_ts = splitted_name[-1].split(".")[0]
        if len(start_ts) == 16:
            start_ts = start_ts[:13]
        if len(end_ts) == 16:
            end_ts = end_ts[:13]
        if int(start_ts) > int(end_ts):
            return False
        
        # at least 5 components; at most 8 components
        if len(splitted_name) < 5 or len(splitted_name) > 8:
            return False
        
        return True

    @staticmethod
    def any_overlap(data_names: list['DataName']) -> bool:
        if not data_names:
            return False
            
        # 按开始时间排序
        sorted_intervals = sorted(
            [(d.start_timestamp, d.end_timestamp) for d in data_names]
        )
        
        # make sure all data names have the same suffix
        suffix = data_names[0].suffix
        if not all(d.suffix == suffix for d in data_names):
            raise ValueError("All data names must have the same suffix")
        
        # 检查相邻区间是否重叠
        for i in range(len(sorted_intervals) - 1):
            if sorted_intervals[i][1] > sorted_intervals[i + 1][0]:
                return True
                
        return False

    @staticmethod
    def group_overlapped(data_names: list['DataName']) -> list[list['DataName']]:
        """Group data names where overlaps are transitive.
        
        Args:
            data_names: List of DataName objects to group
            
        Returns:
            List of lists where each inner list contains DataNames that overlap transitively
            
        Raises:
            ValueError: If data names have different suffixes
        """
        if not data_names:
            return []
            
        # Validate all have same suffix
        suffix = data_names[0].suffix
        if not all(d.suffix == suffix for d in data_names):
            raise ValueError("All data names must have the same suffix")
            
        # Build adjacency list
        adj = {d: [] for d in data_names}
        for i, d1 in enumerate(data_names):
            for d2 in data_names[i+1:]:
                if d1.overlap(d2, check_suffix=False):
                    adj[d1].append(d2)
                    adj[d2].append(d1)
                    
        # Find connected components using DFS
        visited = set()
        groups = []
        
        for d in data_names:
            if d not in visited:
                stack = [d]
                visited.add(d)
                group = []
                
                while stack:
                    current = stack.pop()
                    group.append(current)
                    for neighbor in adj[current]:
                        if neighbor not in visited:
                            visited.add(neighbor)
                            stack.append(neighbor)
                            
                groups.append(group)
                
        return groups

    def __init__(self, name, suffix: str = ""):
        if not DataName.validate_name(name):
            raise ValueError(f"Invalid data name: {name}")
        
        self.name = name
        self.splitted_name = name.split('-')

        self.exchange = self.splitted_name[0]
        self.category = self.splitted_name[1]
        self.start_timestamp = int(self.splitted_name[-2])
        self.end_timestamp = int(self.splitted_name[-1].split(".")[0])
        
        if suffix:
            if self.splitted_name[-1].split(".")[1] != suffix:
                raise ValueError(f"Invalid data name: {name}, suffix must be {suffix}")
        self.id = "-".join(self.splitted_name[2:-2])
        self.prefix = "-".join(self.splitted_name[:-2])
        self.suffix = self.splitted_name[-1].split(".")[1]

    def overlap(self, other, check_suffix: bool = True):
        if check_suffix:
            if self.suffix != other.suffix:
                raise ValueError(f"Data name {self.name} and {other.name} must have the same suffix")
            return not (
                self.start_timestamp > other.end_timestamp or
                self.end_timestamp < other.start_timestamp
            )
        else:
            return not (
                self.start_timestamp > other.end_timestamp or
                self.end_timestamp < other.start_timestamp
            )

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name
    
    def __eq__(self, other):
        return self.name == other.name
    
    def __hash__(self):
        return hash(self.name)
    
    def __len__(self):
        return len(self.name)
    
    def __contains__(self, item):
        return item in self.name
