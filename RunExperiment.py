import io, math, os, os.path, random, shutil, time, timeit

from Catalog.Schema        import DBSchema
from Database              import Database

db = Database(dataDir='./data')

def getResults(query):
  return [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1] ]

def report(queryNum, query):
  start = time.time()
  resultCount = len(getResults(query))
  end = time.time()
  print('Query {} took {:.3f}s and retrieved {} records'.format(queryNum, end - start, resultCount))

joinMethod = 'hash'
#joinMethod = 'block-nested-loops'
isHashJoin = joinMethod == 'hash'

#######################################
# Query 1 (Hash Join)
#######################################

ps_partkey = DBSchema('partsupp', [('PS_PARTKEY', 'int')])
p_partkey = DBSchema('part', [('P_PARTKEY','int')])

ps_suppkey = DBSchema('partsupp', [('PS_SUPPKEY', 'int')])
s_suppkey = DBSchema('supplier', [('S_SUPPKEY','int')])

table1 = db.query().fromTable('partsupp').where('PS_AVAILQTY == 1').select({'PS_PARTKEY':('PS_PARTKEY','int'), 'PS_SUPPKEY':('PS_SUPPKEY','int')}).join( \
             db.query().fromTable('part').select({'P_NAME':('P_NAME','char(55)'), 'P_PARTKEY':('P_PARTKEY','int')}), \
             rhsSchema=DBSchema('part', [('P_NAME','char(55)'), ('P_PARTKEY','int')]), \
             method=joinMethod, \
             expr=None if isHashJoin else 'PS_PARTKEY == P_PARTKEY', \
             lhsHashFn='hash(PS_PARTKEY) % 4', lhsKeySchema = ps_partkey, \
             rhsHashFn='hash(P_PARTKEY) % 4', rhsKeySchema = p_partkey).join( \
                          db.query().fromTable('supplier').select({'S_NAME':('S_NAME','char(25)'), 'S_SUPPKEY':('S_SUPPKEY', 'int')}), \
                          rhsSchema=DBSchema('supplier', [('S_NAME','char(25)'), ('S_SUPPKEY', 'int')]), \
                          method=joinMethod, \
                          expr=None if isHashJoin else 'PS_SUPPKEY == S_SUPPKEY', \
                          lhsHashFn='hash(PS_SUPPKEY) % 4', lhsKeySchema = ps_suppkey, \
                          rhsHashFn='hash(S_SUPPKEY) % 4', rhsKeySchema = s_suppkey, \
                          ).select({'P_NAME':('P_NAME','char(55)'), 'S_NAME':('S_NAME','char(25)')})

table2 = db.query().fromTable('partsupp').where('PS_SUPPLYCOST < 5').select({'PS_PARTKEY':('PS_PARTKEY','int'), 'PS_SUPPKEY':('PS_SUPPKEY','int')}).join( \
             db.query().fromTable('part').select({'P_NAME':('P_NAME','char(55)'), 'P_PARTKEY':('P_PARTKEY','int')}), \
             rhsSchema=DBSchema('part', [('P_NAME','char(55)'), ('P_PARTKEY','int')]), \
             method=joinMethod, \
             expr=None if isHashJoin else 'PS_PARTKEY == P_PARTKEY', \
             lhsHashFn='hash(PS_PARTKEY) % 4', lhsKeySchema = ps_partkey, \
             rhsHashFn='hash(P_PARTKEY) % 4', rhsKeySchema = p_partkey).join( \
                          db.query().fromTable('supplier').select({'S_NAME':('S_NAME','char(25)'), 'S_SUPPKEY':('S_SUPPKEY', 'int')}), \
                          rhsSchema=DBSchema('supplier', [('S_NAME','char(25)'), ('S_SUPPKEY', 'int')]), \
                          method=joinMethod, \
                          expr=None if isHashJoin else 'PS_SUPPKEY == S_SUPPKEY', \
                          lhsHashFn='hash(PS_SUPPKEY) % 4', lhsKeySchema = ps_suppkey, \
                          rhsHashFn='hash(S_SUPPKEY) % 4', rhsKeySchema = s_suppkey, \
                          ).select({'P_NAME':('P_NAME','char(55)'), 'S_NAME':('S_NAME','char(25)')})

query = table1.union(table2).finalize()

report(1, query)

#######################################
# Query 2 (Hash Join)
#######################################

query = db.query().fromTable('part').select({'P_NAME': ('P_NAME', 'char(55)'), 'P_PARTKEY': ('P_PARTKEY', 'int')}).join( \
             db.query().fromTable('lineitem').where("L_RETURNFLAG == 'R'"), \
             method='hash', \
             expr=None if isHashJoin else 'P_PARTKEY == L_PARTKEY', \
             lhsHashFn='hash(P_PARTKEY) % 10', lhsKeySchema=DBSchema('partkey1',[('P_PARTKEY', 'int')]), \
             rhsHashFn='hash(L_PARTKEY) % 10', rhsKeySchema=DBSchema('partkey2',[('L_PARTKEY', 'int')])).groupBy( \
                          groupSchema=DBSchema('groupByKey', [('P_NAME', 'char(55)')]), \
                          aggSchema=DBSchema('groupBy', [('count','int')]), \
                          groupExpr=(lambda e: e.P_NAME), \
                          aggExprs=[(0, lambda acc, e: acc + 1, lambda x: x)], \
                          groupHashFn=(lambda gbVal: hash(gbVal[0]) % 10)).finalize()
          
report(2, query)

#######################################
# Query 3 (Hash Join)
#######################################

temp = db.query().fromTable('nation').select({'N_NATIONKEY':('N_NATIONKEY','int'), 'N_NAME':('N_NAME', 'char(25)')}).join( \
             db.query().fromTable('customer').select({'C_NATIONKEY':('C_NATIONKEY','int'),'C_CUSTKEY':('C_CUSTKEY','int')}), \
             rhsSchema=DBSchema('c',[('C_NATIONKEY','int'),('C_CUSTKEY','int')]), \
             method=joinMethod, \
             expr=None if isHashJoin else 'N_NATIONKEY == C_NATIONKEY', \
             lhsHashFn='hash(N_NATIONKEY) % 5', lhsKeySchema=DBSchema('lKey1',[('N_NATIONKEY','int')]), \
             rhsHashFn='hash(C_NATIONKEY) % 5', rhsKeySchema=DBSchema('rKey1',[('C_NATIONKEY','int')])).join( \
                          db.query().fromTable('orders').select({'O_ORDERKEY':('O_ORDERKEY','int'), 'O_CUSTKEY':('O_CUSTKEY','int')}), \
                          method=joinMethod, \
                          expr=None if isHashJoin else 'C_CUSTKEY == O_CUSTKEY', \
                          lhsHashFn='hash(C_CUSTKEY) % 5', lhsKeySchema=DBSchema('lKey2',[('C_CUSTKEY','int')]), \
                          rhsHashFn='hash(O_CUSTKEY) % 5', rhsKeySchema=DBSchema('rKey2',[('O_CUSTKEY','int')])).join( \
                                       db.query().fromTable('lineitem').select({'L_ORDERKEY':('L_ORDERKEY','int'),'L_PARTKEY':('L_PARTKEY','int'), 'L_QUANTITY':('L_QUANTITY','float')}), \
                                       rhsSchema=DBSchema('l',[('L_ORDERKEY','int'),('L_PARTKEY','int'),('L_QUANTITY','float')]), \
                                       method=joinMethod, \
                                       expr=None if isHashJoin else 'O_ORDERKEY == L_ORDERKEY', \
                                       lhsHashFn='hash(O_ORDERKEY) % 5', lhsKeySchema=DBSchema('lKey3',[('O_ORDERKEY','int')]), \
                                       rhsHashFn='hash(L_ORDERKEY) % 5', rhsKeySchema=DBSchema('rKey3',[('L_ORDERKEY','int')])).join( \
                                                    db.query().fromTable('part').select({'P_PARTKEY':('P_PARTKEY','int'), 'P_NAME':('P_NAME','char(55)')}), \
                                                    rhsSchema=DBSchema('p', [('P_PARTKEY','int'),('P_NAME','char(55)')]),\
                                                    method=joinMethod, \
                                                    expr=None if isHashJoin else 'L_PARTKEY == P_PARTKEY', \
                                                    lhsHashFn='hash(L_PARTKEY) % 5', lhsKeySchema=DBSchema('lKey4',[('L_PARTKEY','int')]), \
                                                    rhsHashFn='hash(P_PARTKEY) % 5', rhsKeySchema=DBSchema('rKey4',[('P_PARTKEY','int')])) \
             .groupBy( \
             groupSchema=DBSchema('gbSchema1',[('N_NAME','char(25)'), ('P_NAME','char(55)')]), \
             aggSchema=DBSchema('aggSchema1',[('num','float')]), \
             groupExpr=(lambda e: (e.N_NAME, e.P_NAME)), \
             aggExprs=[(0, lambda acc, e: acc + e.L_QUANTITY, lambda x: x)], \
             groupHashFn=(lambda gbVal: hash(gbVal[0]) % 10))

query = temp.groupBy( \
             groupSchema=DBSchema('gbSchema2',[('N_NAME','char(25)')]), \
             aggSchema=DBSchema('aggSchema2',[('max','float')]), \
             groupExpr=(lambda e: e.N_NAME), \
             aggExprs=[(0, lambda acc, e: max(acc, e.num), lambda x: x)], \
             groupHashFn=(lambda gbVal: hash(gbVal[0]) % 10)).finalize()

report(3, query)       
                 
db.close()