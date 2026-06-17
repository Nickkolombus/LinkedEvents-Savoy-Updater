import requests, json
q='Sleeping At Last'
resp=requests.get('https://api.hel.fi/linkedevents/v1/event/', params={'text':q,'page_size':100})
print('status', resp.status_code)
data=resp.json().get('data',[])
print('count', len(data))
for e in data:
    name = e.get('name') or {}
    display = name.get('fi') or name.get('en') or name.get('sv') or 'N/A'
    print('TITLE:', display)
    print('ID:', e.get('@id') or e.get('id'))
    print('START:', e.get('start_time'))
    print('END:', e.get('end_time'))
    print('STATUS:', e.get('event_status'))
    loc = e.get('location')
    if isinstance(loc, list):
        print('PLACES:', [l.get('name') if isinstance(l, dict) else l for l in loc])
    else:
        print('PLACE:', loc)
    print('DESCRIPTION SNIPPET:', (e.get('description') or {}).get('fi', '')[:120])
    print('---')

# save for inspection
with open('sleeping_results.json','w',encoding='utf-8') as f:
    json.dump([{
        'title': e.get('name'), 'start': e.get('start_time'), 'status': e.get('event_status'), 'id': e.get('@id')
    } for e in data], f, ensure_ascii=False, indent=2)
print('Wrote sleeping_results.json')
