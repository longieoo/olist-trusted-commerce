"""Independent CSV/Decimal oracle for the accepted financial population.

Does not reuse transformation SQL. Quarantine policy is intentionally not reimplemented.
"""
import argparse
import csv
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import duckdb
from olist.fixtures import verified_manifest
from olist.local_warehouse import resolve_publication


def verify(directory,root):
    source=verified_manifest(directory)
    path,receipt=resolve_publication(root)
    if source['dataset_version']!=receipt['dataset_version']:
        raise ValueError('Source and warehouse version differ')
    totals=defaultdict(lambda:[Decimal(0),Decimal(0),Decimal(0)])
    checked={}
    with duckdb.connect(str(path),read_only=True) as conn:
        for table,filename,keys,fields,slots in [
            ('fact_order_items','olist_order_items_dataset.csv',['order_id','order_item_id'],['price','freight_value'],[0,1]),
            ('fact_payments','olist_order_payments_dataset.csv',['order_id','payment_sequential'],['payment_value'],[2])]:
            raw=defaultdict(set)
            with (Path(directory)/filename).open(encoding='utf-8-sig',newline='') as f:
                for row in csv.DictReader(f):
                    raw[tuple(row[k] for k in keys)].add(tuple(Decimal(row[k]) for k in fields))
            rows=conn.execute(f"SELECT {','.join(keys+fields)} FROM {table}").fetchall()
            for row in rows:
                key=tuple(str(v) for v in row[:len(keys)])
                values=tuple(row[len(keys):])
                if values not in raw[key]: raise AssertionError('Accepted value not present in raw source')
                for slot,value in zip(slots,values):totals[key[0]][slot]+=value
            checked[table]=len(rows)
        orders=conn.execute('SELECT order_id,merchandise_brl,freight_brl,payment_brl FROM fact_orders').fetchall()
        mismatches=[row[0] for row in orders if list(row[1:])!=totals[row[0]]]
        if mismatches: raise AssertionError(f'{len(mismatches)} orders mismatch')
    result={'status':'passed','dataset_version':source['dataset_version'],'generation':receipt['generation'],
        'orders_checked':len(orders),'source_backed_rows':checked,'per_order_mismatches':0,
        'scope':'Raw CSV value membership for accepted items/payments; independent Decimal per-order sums. Does not independently validate quarantine selection.'}
    Path('evidence/final-source-oracle.json').write_text(json.dumps(result,indent=2))
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--directory',default='data/real');parser.add_argument('--root',default='data/local')
    args=parser.parse_args();print(json.dumps(verify(args.directory,args.root),indent=2))
