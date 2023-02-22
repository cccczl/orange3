from Orange.data import filter


class IsDefinedSql(filter.IsDefined):
    InheritEq = True

    def to_sql(self):
        sql = " AND ".join([f'{column} IS NOT NULL' for column in self.columns])
        if self.negate:
            sql = f'NOT ({sql})'
        return sql


class SameValueSql(filter.SameValue):
    def to_sql(self):
        if self.value is None:
            sql = f'{self.column} IS NULL'
        else:
            sql = f"{self.column} = {self.value}"
        if self.negate:
            if self.value is None:
                sql = f'NOT ({sql})'
            else:
                sql = f'(NOT ({sql}) OR {self.column} is NULL)'
        return sql


class ValuesSql(filter.Values):
    def to_sql(self):
        aggregator = " AND " if self.conjunction else " OR "
        sql = aggregator.join(c.to_sql() for c in self.conditions)
        if self.negate:
            sql = f'NOT ({sql})'
        return sql if self.conjunction else f'({sql})'


class FilterDiscreteSql(filter.FilterDiscrete):
    def to_sql(self):
        if self.values is not None:
            return f"{self.column} IN ({','.join(self.values)})"
        else:
            return f"{self.column} IS NOT NULL"


class FilterContinuousSql(filter.FilterContinuous):
    def to_sql(self):
        if self.oper == self.Equal:
            return f"{self.column} = {self.ref}"
        elif self.oper == self.NotEqual:
            return f"{self.column} <> {self.ref} OR {self.column} IS NULL"
        elif self.oper == self.Less:
            return f"{self.column} < {self.ref}"
        elif self.oper == self.LessEqual:
            return f"{self.column} <= {self.ref}"
        elif self.oper == self.Greater:
            return f"{self.column} > {self.ref}"
        elif self.oper == self.GreaterEqual:
            return f"{self.column} >= {self.ref}"
        elif self.oper == self.Between:
            return f"{self.column} >= {self.ref} AND {self.column} <= {self.max}"
        elif self.oper == self.Outside:
            return f"({self.column} < {self.ref} OR {self.column} > {self.max})"
        elif self.oper == self.IsDefined:
            return f"{self.column} IS NOT NULL"
        else:
            raise ValueError("Invalid operator")


class FilterString(filter.FilterString):
    def to_sql(self):
        if self.oper == self.IsDefined:
            return f"{self.column} IS NOT NULL"
        if self.case_sensitive:
            field = self.column
            value = self.ref
        else:
            field = f'LOWER({self.column})'
            value = self.ref.lower()
        if self.oper == self.Equal:
            return f"{field} = {quote(value)}"
        elif self.oper == self.NotEqual:
            return f"{field} <> {quote(value)} OR {field} IS NULL"
        elif self.oper == self.Less:
            return f"{field} < {quote(value)}"
        elif self.oper == self.LessEqual:
            return f"{field} <= {quote(value)}"
        elif self.oper == self.Greater:
            return f"{field} > {quote(value)}"
        elif self.oper == self.GreaterEqual:
            return f"{field} >= {quote(value)}"
        elif self.oper == self.Between:
            high = quote(self.max if self.case_sensitive else self.max.lower())
            return f"{field} >= {quote(value)} AND {field} <= {high}"
        elif self.oper == self.Outside:
            high = quote(self.max if self.case_sensitive else self.max.lower())
            return f"({field} < {quote(value)} OR {field} > {high})"
        elif self.oper == self.Contains:
            return "%s LIKE '%%%s%%'" % (field, value)
        elif self.oper == self.StartsWith:
            return "%s LIKE '%s%%'" % (field, value)
        elif self.oper == self.EndsWith:
            return "%s LIKE '%%%s'" % (field, value)
        else:
            raise ValueError("Invalid operator")


class FilterStringList(filter.FilterStringList):
    def to_sql(self):
        values = self.values
        if not self.case_sensitive:
            values = map(lambda x: x.lower(), values)
            sql = "LOWER(%s) in (%s)"
        else:
            sql = "%s in (%s)"
        return sql % (self.column, ", ".join(map(quote, values)))


def quote(value):
    return f"'{value}'" if isinstance(value, str) else value


class CustomFilterSql(filter.Filter):
    def __init__(self, where_sql, negate=False):
        super().__init__(negate=negate)
        self.sql = where_sql

    def to_sql(self):
        return f"NOT ({self.sql})" if self.negate else f"({self.sql})"
