"""Package measured JSON and charts; refuses replacing previous evidence."""
import json
import shutil
from pathlib import Path
from scripts.analyze_completion_snapshot import bars

ROOT=Path(__file__).resolve().parents[1]
out=ROOT/'evidence/completion_20261016_v1/results'
out.mkdir(parents=True,exist_ok=False)
measurements=[]
for mode in ('local','standalone'):
    source=ROOT/f'runtime/completion_benchmark_20261016_{mode}_v1/result.json'
    data=json.loads(source.read_text())
    assert data['status']=='ok' and data['input_unchanged'] and data['article_count']==13388
    shutil.copy2(source,out/(mode+'_benchmark.json'))
    measurements.append(data)
assert measurements[0]['aggregate_sha256']==measurements[1]['aggregate_sha256']
cluster=ROOT/'runtime/completion_cluster_20261016_v1/summary.json'
data=json.loads(cluster.read_text(encoding='utf-8'))
assert len(data['results'])==6 and all(r['save_reload_mismatches']==0 and r['input_count']==13388 for r in data['results'])
shutil.copy2(cluster,out/'clustering.json')
rows=[dict(name=f'PCA {r["feature_config"]["pca_dim"] or "none"} / k={r["k"]}',count=round(r['silhouette'],4)) for r in data['results']]
with (out/'silhouette.svg').open('x',encoding='utf-8') as handle:
    handle.write(bars(rows,'Silhouette tren snapshot 13.388 bai - khong phai sentiment accuracy'))
# Elbow cost is compared only inside one feature space, not across PCA/TF-IDF.
for dim in (None,16):
    rows=[dict(name=f'k={r["k"]}',count=round(r['training_cost'],2)) for r in data['results'] if r['feature_config']['pca_dim']==dim]
    with (out/f'cost_pca{dim or 0}.svg').open('x',encoding='utf-8') as handle:
        handle.write(bars(rows,f'Training cost (elbow) - PCA {dim or "none"}'))
summary=dict(local_median=measurements[0]['median_seconds'],standalone_median=measurements[1]['median_seconds'],aggregate_equal=True,
             highest_silhouette_config=data['highest_silhouette_config'],clusters=[dict(name=r['name'],silhouette=r['silhouette'],training_cost=r['training_cost'],sizes=r['sizes']) for r in data['results']])
with (out/'summary.json').open('x',encoding='utf-8') as handle:
    json.dump(summary,handle,indent=2)
print(json.dumps(summary))
