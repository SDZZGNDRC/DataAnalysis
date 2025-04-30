def map(dps):
    try:
        res = []
        for dp in dps:
            new_data = {}
            new_dp = {}
            if 'event' in dp and dp['event'] == 'notice' and 'The connection will soon be closed for a service upgrade' in dp['msg']:
                continue
            if 'details' in dp['data'][0]:
                new_data['details'] = dp['data'][0]['details'][0]
            if 'instId' in dp['data'][0]:
                new_data['instId'] = dp['data'][0]['instId']
            if 'instType' in dp['data'][0]:
                new_data['instType'] = dp['data'][0]['instType']
            if 'uly' in dp['data'][0]:
                new_data['uly'] = dp['data'][0]['uly']
            new_dp = {
                "arg": dp['arg'],
                "data": new_data,
                "ts": int(dp['data'][0]['details'][0]['ts'])
            }
            res.append(new_dp)
        return sorted(res,key=lambda dp: dp['ts'])
    except KeyError as e:
        return sorted(res,key=lambda dp: dp['ts'])

