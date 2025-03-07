books_schema = {
    "type": "object",
    "required": ["elapsedTime", "data"],
    "properties": {
        "elapsedTime": {
            "type": "array",
            "items": {"type": "integer"},
            "minItems": 2,
            "maxItems": 2
        },
        "data": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["arg", "data", "action"],
                "properties": {
                    "arg": {
                        "type": "object",
                        "properties": {
                            "channel": {"type": "string"},
                            "instId": {"type": "string"},
                            "instType": {"type": "string"},
                            "InstFamily": {"type": "string"},
                            "uly": {"type": "string"}
                        },
                        "required": ["channel"]
                    },
                    "data": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["asks", "bids", "ts", "checksum", "prevSeqId", "seqId"],
                            "properties": {
                                "asks": {
                                    "type": "array",
                                    "items": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "minItems": 4,
                                        "maxItems": 4
                                    }
                                },
                                "bids": {
                                    "type": "array",
                                    "items": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "minItems": 4,
                                        "maxItems": 4
                                    }
                                },
                                "ts": {
                                    "type": "string",
                                    "pattern": "^\\d+$"
                                },
                                "checksum": {"type": "integer"},
                                "prevSeqId": {"type": "integer"},
                                "seqId": {"type": "integer"}
                            }
                        }
                    },
                    "action": {"type": "string"}
                }
            }
        }
    }
}