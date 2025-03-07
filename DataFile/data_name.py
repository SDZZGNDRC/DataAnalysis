class DataName:
    # example: OKX-Books-1INCH-USD-SWAP-400-1689297329268-1689298999939.7z
    @staticmethod
    def validate_name(name: str) -> bool:
        # endswith `.7z` or `.json`
        if not name.endswith('.7z') and not name.endswith('.json'):
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
        if int(splitted_name[-2]) > int(splitted_name[-1].split(".")[0]):
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
        
        # 检查相邻区间是否重叠
        for i in range(len(sorted_intervals) - 1):
            if sorted_intervals[i][1] >= sorted_intervals[i + 1][0]:
                return True
                
        return False

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
        

    def overlap(self, other):
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
    
