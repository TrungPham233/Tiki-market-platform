import json
from pathlib import Path

pages_dir = Path('E:/baitap/Project1/tiki-market-platform/dashboard/tiki_market_analytics.Report/definition/pages')

with open(pages_dir / 'pages.json', encoding='utf-8') as f:
    meta = json.load(f)

for page_id in meta['pageOrder']:
    page_dir = pages_dir / page_id
    page_json = page_dir / 'page.json'
    pdata = json.loads(page_json.read_text(encoding='utf-8'))
    name = pdata['displayName']
    print('=== ' + name + ' ===')
    vis_dir = page_dir / 'visuals'
    if vis_dir.exists():
        for vd in sorted(vis_dir.iterdir()):
            vf = vd / 'visual.json'
            if vf.exists():
                vdata = json.loads(vf.read_text(encoding='utf-8'))
                vtype = vdata['visual']['visualType']
                pos = vdata['position']
                qs = vdata['visual'].get('query', {}).get('queryState', {})
                roles = list(qs.keys())
                print('  ' + vd.name + ' | ' + vtype + ' | y=' + str(pos['y']) + ' h=' + str(pos['height']) + ' | roles=' + str(roles))
    print()

