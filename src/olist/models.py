"""One SQL source for full rebuilds, impacted-order refreshes and dbt export."""
import re
from pathlib import Path

SQL = Path(__file__).resolve().parents[2] / 'sql' / 'local_marts.sql'
ORDER_MODELS = {'fact_order_items', 'fact_payments', 'bridge_order_seller',
                'order_review_summary', 'fact_orders', 'fact_order_geography'}


def definitions():
    text = re.sub(r'--[^\n]*', '', SQL.read_text(encoding='utf-8'))
    return [(m.group(1), m.group(2).strip()) for m in re.finditer(
        r'CREATE TABLE (\w+) AS\s*(.*?);', text, re.S | re.I)]


def build(conn, incremental=False):
    for name, query in definitions():
        if incremental and name.startswith('dim_'):
            continue  # Dimensions are immutable within a simulation namespace.
        if incremental and name in ORDER_MODELS:
            conn.execute(f'DELETE FROM {name} WHERE order_id IN (SELECT order_id FROM impacted_orders)')
            conn.execute(f'INSERT INTO {name} SELECT * FROM ({query}) q WHERE order_id IN (SELECT order_id FROM impacted_orders)')
        else:
            conn.execute(f'CREATE OR REPLACE TABLE {name} AS {query}')


def export_dbt(root):
    """Generated project: regenerate from SQL; never edit generated model copies."""
    import yaml
    from .contracts import HEADERS
    root = Path(root); models = root/'models'; models.mkdir(parents=True, exist_ok=True)
    defs = definitions(); names = {n for n, _ in defs}
    for name, query in defs:
        for relation in sorted(names | {'silver_'+t for t in HEADERS}, key=len, reverse=True):
            expression = "{{ source('canonical', '"+relation+"') }}" if relation.startswith('silver_') else "{{ ref('"+relation+"') }}"
            query = re.sub(r'\b'+relation+r'\b', lambda _: expression, query)
        (models/(name+'.sql')).write_text('-- GENERATED from sql/local_marts.sql\n'+query+'\n', encoding='utf-8')
    (root/'dbt_project.yml').write_text(yaml.safe_dump({'name':'olist_local','version':'1.0.0','config-version':2,
        'profile':'olist_local','model-paths':['models'],'models':{'olist_local':{'+materialized':'table','+schema':'analytics'}}}), encoding='utf-8')
    (root/'profiles.yml').write_text("olist_local:\n  target: local\n  outputs:\n    local:\n      type: duckdb\n      path: \"{{ env_var('OLIST_DBT_DATABASE') }}\"\n      threads: 2\n", encoding='utf-8')
    (models/'schema.yml').write_text(yaml.safe_dump({'version':2,'sources':[{'name':'canonical','schema':'main',
        'tables':[{'name':'silver_'+t} for t in HEADERS]}], 'models':[
            {'name':'fact_orders','columns':[{'name':'order_id','tests':['unique','not_null']}]}]}), encoding='utf-8')
    return str(root)
