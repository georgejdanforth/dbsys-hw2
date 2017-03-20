import time

from Database import Database
from Catalog.Schema import DBSchema


db = Database(datadir="./data")


def getResults(query):
    return [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1]]


def query1(joinMethod):

    def get_table(whereStmt):
        table = db.query()\
            .fromTable("partsupp")\
            .where( whereStmt )\
            .select({
                "PS_PARTKEY": ("PS_PARTKEY", "int"),
                "PS_SUPPKEY": ("PS_SUPPKEY", "int")
            })\
            .join(
                db.query()\
                    .fromTable("part")\
                    .select({
                        "P_NAME": ("P_NAME", "char(55)"),
                        "P_PARTKEY": ("P_PARTKEY", "int")
                    }),
                rhsSchema=DBSchema("part", [("P_NAME", "char(55)"), ("P_PARTKEY", "int")]),
                method=joinMethod,
                expr=None if joinMethod == "hash" else "PS_PARTKEY == P_PARTKEY",
                lhsHashFN="hash(PS_PARTKEY) % 4",
                lhsKeySchema=DBSchema("partsupp", [("PS_PARTKEY", "int")]),
                rhsHashFN="hash(P_PARTKEY) % 4",
                rhsKeySchema=DBSchema("part", [("P_PARTKEY", "int")])
            )\
            .join(
                db.query()\
                    .fromTable("supplier")\
                    .select({
                        "S_NAME": ("S_NAME", "char(25)"),
                        "S_SUPPKEY": ("S_SUPPKEY", "int")
                    }),
                rhsSchema=DBSchema("supplier", [("S_NAME", "char(25)"), ("S_SUPPKEY", "int")]),
                method=joinMethod,
                expr=None if joinMethod == "hash" else "PS_SUPPKEY == S_SUPPKEY",
                lhsHashFN="hash(PS_SUPPKEY) % 4",
                lhsKeySchema=DBSchema("partsupp", [("PS_SUPPKEY", "int")]),
                rhsHashFN="hash(S_SUPPKEY) % 4",
                rhsKeySchema=DBSchema("supplier", [("S_SUPPKEY", "int")])
            )\
            .select({
                "P_NAME": ("P_NAME", "char(55)"),
                "S_NAME": ("S_NAME", "char(25)")
            })

    table1 = get_table("PS_AVAILQTY == 1")
    table2 = get_table("PS_SUPPLYCOST < 5")

    query = table1.union(table2).finalize()

    return query


def main():
    print(getResults(query1("hash")))


if __name__ == "__main__":
    main()
