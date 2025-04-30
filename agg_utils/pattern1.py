def map(dps):
    try:
        return sorted([
            {
                "arg": dp['arg'],
                "data": dp['data'][0],
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
                    "ts": int(dp['data'][0]['ts'])
                })
            elif 'event' in dp and dp['event'] == 'notice' and 'The connection will soon be closed for a service upgrade' in dp['msg']:
                continue
            else:
                raise e
        return sorted(res,key=lambda dp: dp['ts'])

