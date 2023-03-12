import meilisearch


from typing import Optional
import sqlite3
import argparse
from markdownify import markdownify

tables_to_import = [
    "feed",
    "category",
    "entry",
]
yield_size = 500

# create a parser object
parser = argparse.ArgumentParser()

# add arguments to the parser
parser.add_argument("--meili", help="meilisearch api url (Default: http://localhost:7700)", default="http://localhost:7700")
parser.add_argument("--key", help="api key", default=None)
parser.add_argument("--sql", help="sqlite file path",type=str)
parser.add_argument("--skip", help="skip to row",type=int, default=0)


# parse the arguments from standard input
args = parser.parse_args()

meili_url: str = args.meili
meili_key: Optional[str] = args.key
sql_path:  str = args.sql
skip_to: int = args.skip


# connect to meilisearch
ml_client = meilisearch.Client(meili_url, meili_key)

# connect to sqlite
sqlconn = sqlite3.connect(sql_path)
sqlconn.text_factory = lambda b: b.decode(errors = 'ignore')
sqlc = sqlconn.cursor()

# get all tables from sqlite
sqlc.execute("SELECT name FROM sqlite_master WHERE type='table';")
sql_tables = sqlc.fetchall()

# get table data
def get_table_data(table_name: str):
    sqlc.execute(f"SELECT * FROM {table_name}")
    table_data = sqlc.fetchall()
    columns = [column[0] for column in sqlc.description]
    print("table:", table_name, len(table_data))

    return table_data, columns

# clean table data
def clean_table_data(table, columns, table_name, skip_to=0):
    i = 1
    for row in table:
        if i < skip_to:
            i += 1
            continue

        json_data = []
        dict_json = dict(zip(columns, row))
        # remove `hash` key
        if "hash" in dict_json:
            del dict_json["hash"]
        if "content" in dict_json and table_name == "entry":
            dict_json["content"] = markdownify(html=dict_json["content"])
        yield dict_json

def table_to_json(table_name: str):
    table_data, columns = get_table_data(table_name)
    pack = []
    count = 0
    for row in clean_table_data(table_data, columns, table_name, skip_to=skip_to):
        count += 1
        print(count, end="\r")
        if count % 1000 == 0:
            print("imported:", count, "skiped:", skip_to, "total:", count+skip_to)
        pack.append(row)
        if len(pack) == yield_size:
            yield pack
            pack = []

# import tables to meilisearch
def import_table(table: str):
    if (table,) not in sql_tables:
        print("table not found:", table)
        return False

    print("importing:", table)
    for pack in table_to_json(table):
        ml_client.index(table).add_documents(
            documents=pack,
            primary_key="id"
        )

    return True

# ml_client.index("entry").delete()

for table in tables_to_import:
    import_table(table)

print("done")