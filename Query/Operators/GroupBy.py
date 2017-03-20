import itertools as it

from Catalog.Schema import DBSchema
from Query.Operator import Operator


class GroupBy(Operator):
    def __init__(self, subPlan, **kwargs):
        super().__init__(**kwargs)

        if self.pipelined:
            raise ValueError("Pipelined group-by-aggregate operator not supported")

        self.subPlan = subPlan
        self.subSchema = subPlan.schema()
        self.groupSchema = kwargs.get("groupSchema", None)
        self.aggSchema = kwargs.get("aggSchema", None)
        self.groupExpr = kwargs.get("groupExpr", None)
        self.aggExprs = kwargs.get("aggExprs", None)
        self.groupHashFn = kwargs.get("groupHashFn", None)

        self.validateGroupBy()
        self.initializeSchema()

    # Perform some basic checking on the group-by operator's parameters.
    def validateGroupBy(self):
        requireAllValid = [self.subPlan, \
                           self.groupSchema, self.aggSchema, \
                           self.groupExpr, self.aggExprs, self.groupHashFn]

        if any(map(lambda x: x is None, requireAllValid)):
            raise ValueError("Incomplete group-by specification, missing a required parameter")

        if not self.aggExprs:
            raise ValueError("Group-by needs at least one aggregate expression")

        if len(self.aggExprs) != len(self.aggSchema.fields):
            raise ValueError("Invalid aggregate fields: schema mismatch")

    # Initializes the group-by's schema as a concatenation of the group-by
    # fields and all aggregate fields.
    def initializeSchema(self):
        schema = self.operatorType() + str(self.id())
        fields = self.groupSchema.schema() + self.aggSchema.schema()
        self.outputSchema = DBSchema(schema, fields)

    # Returns the output schema of this operator
    def schema(self):
        return self.outputSchema

    # Returns any input schemas for the operator if present
    def inputSchemas(self):
        return [self.subPlan.schema()]

    # Returns a string describing the operator type
    def operatorType(self):
        return "GroupBy"

    # Returns child operators if present
    def inputs(self):
        return [self.subPlan]

    # Iterator abstraction for selection operator.
    def __iter__(self):
        self.initializeOutput()
        self.outputIterator = self.processAllPages()

        return self

    def __next__(self):
        return next(self.outputIterator)

    # Page-at-a-time operator processing
    def processInputPage(self, pageId, page):
        raise ValueError("Page-at-a-time processing not supported for joins")

    # Set-at-a-time operator processing
    def processAllPages(self):

        relations = []

        for (pageId, page) in iter(self.subPlan):
            for tup in page:

                unpackedTup = self.subSchema.unpack(tup)
                groupByVal = tuple([self.groupExpr(unpackedTup)])
                hashVal = str(self.groupHashFn(groupByVal))

                if hashVal not in relations:
                    self.storage.createRelation(hashVal, self.subSchema)
                    relations.append(hashVal)

                self.storage.insertTuple(hashVal, tup)

        for rel in relations:
            for (pageId, page) in self.storage.pages(rel):
                groups = {}
                for tup in page:

                    unpackedTup = self.subSchema.unpack(tup)
                    groupByVal = tuple([self.groupExpr(unpackedTup)])

                    if groupByVal not in groups.keys():
                        groups[groupByVal] = [aggExpr[0] for aggExpr in self.aggExprs]

                    for i, aggExpr in enumerate(self.aggExprs):
                        groups[groupByVal][i] = aggExpr[1](groups[groupByVal][i], unpackedTup)

                for groupByVal in groups.keys():
                    for i, aggExpr in enumerate(self.aggExprs):
                        groups[groupByVal][i] = aggExpr[2](groups[groupByVal][i])

                    outputTuple = self.outputSchema.instantiate(
                        *it.chain(list(groupByVal), groups[groupByVal])
                    )
                    self.emitOutputTuple(self.outputSchema.pack(outputTuple))

                if self.outputPages:
                    self.outputPages = [self.outputPages[-1]]

        for rel in relations:
            self.storage.removeRelation(rel)

        return self.storage.pages(self.relationId())


    # Plan and statistics information

    # Returns a single line description of the operator.
    def explain(self):
        return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
               + ", aggSchema=" + self.aggSchema.toString() + ")"
