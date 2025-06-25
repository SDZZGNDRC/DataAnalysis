def map(dps):
    try:
        if any(not dp['data'] for dp in dps):
            dps = list(filter(lambda dp: dp['data'] is not None, dps))
        if any(len(dp['data']) != 1 for dp in dps):
            raise Exception(f"Books.map: the length of dp['data'] is {len(dp['data'])}")
        return sorted([
            {
                "arg": dp['arg'],
                "data": dp['data'][0],
                "action": dp['action'],
                "ts": int(dp['data'][0]['ts'])
            }
            for dp in dps
        ],key=lambda dp: dp['ts'])
    except KeyError as e:
        res = []
        for dp in dps:
            if 'arg' in dp:
                res.append({
                    "arg": dp['arg'],
                    "data": dp['data'][0],
                    "action": dp['action'],
                    "ts": int(dp['data'][0]['ts'])
                })
            elif 'event' in dp and dp['event'] == 'notice' and 'The connection will soon be closed for a service upgrade' in dp['msg']:
                continue
            else:
                raise e
        return sorted(res,key=lambda dp: dp['ts'])
    except Exception as e:
        raise Exception(f'Books.map: {e}')



