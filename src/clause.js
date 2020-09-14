import * as _ from "underscore";

const operators = {
    $gt: '> ?',
    $gte: '>= ?',
    $lt: '< ?',
    $lte: '<= ?',
    $not: '!= ?',
    $in: 'IN (?)',
    $notIn: 'NOT IN (?)',
    $like: 'LIKE ?',
    $notLike: 'NOT LIKE ?',
    $between: 'BETWEEN ? AND ?',
    $notBetween: 'NOT BETWEEN ? AND ?',
    $exist: 'IS NOT NULL',
    $isNull: 'IS NULL'
};

/**
 Transform the condition into a valid SQL `where` clause query string.

 @param {object} where
    The standard object format should contain a property name as the table's column name and it's corresponding
    value is the conditional value to met.

    If using an operator, the value should be on object format where the property name is the name of the operator
    and it's value is the conditional value to met.
    Example:
        {age: {$gt: 5}} = `age > ?`
        {name: {$like: "*me*"}} = `name LIKE ?`

 @param {array} format
    A list of data which holds the actual value of a columns.
 @param {string} table

 @returns {string}
**/
export function whereClause(where, format, table) {
    let sql = [];

    for(const key of Object.keys(where)) {
        const value = where[key];

        if ("$and" === key) {
            const $and = [];

            for(const $orWhere of value) {
                $and.push(whereClause($orWhere, format, table));
            }

            sql.push(`(${$and.join(" AND ")})`);
            continue;
        }

        if ("$or" === key) {
            const $or = [];

            for(const $orWhere of value) {
                $or.push(whereClause($orWhere, format, table));
            }

            sql.push(`(${$or.join(" OR ")})`);

            continue;
        }

        if (_.isObject(value)) {
            for(const con of Object.keys(value)) {
                const _value = value[con],
                    ops = operators[con];

                if (!ops) {
                    continue;
                }

                sql.push(`${key} ${ops}`);

                if (_.contains(["$like", "$notLike"], con)) {
                    format.push(_value.replace(/\*/g, '%'));

                    continue;
                }

                format.push(_value);
            }

            continue;
        }

        sql.push(`${table}.${key} = ?`);
        format.push(value);
    }

    return sql.join(" AND ");
}

/**
 Transforms the order condition into a valid SQL query string.

 @param {string|object} column
    The name of the column to sort the result set to or an object where the property name is the name of the column
    and it's corresponding value is the sort order of the result set.
 @param {string} sortOrder
 @param {string} table

 @returns {string}
**/
export function orderBy(column, sortOrder = "DESC", table = false) {
    const fromStr = (col, order) => table ? `${table}.${col} ${order}` : `${col} ${order}`;

    if (_.isObject(column)) {
        const cols = [];

        for(const col of _.keys(column)) {
            cols.push(fromStr(col, column[col]));
        }

        return `(${cols.join(", ")})`;
    }

    return fromStr(column, sortOrder);
}

/**
 Sets results limit in a query.

 @param {int} page
 @param {int} perPage
 @returns {string}
**/
export function limit(page = 1, perPage = 50) {
    let offset = page * perPage - perPage;

    return ` LIMIT ${offset}, ${perPage}`;
}

/**
 Transforms the group condition into a valid SQL query.

 @param {string|array<*>|object} column
    The name of the column, list of columns or an object which defines how the result is group at.
    
    If a column is an object, the object's property key must be the name of the column and it's value
    may be a group order i.e. `ASC`, `DESC` or the name of an aggregiate function to use to group the result
    set.
 @param {string} groupOrder
 @param {string} table
    The name of the table collection where the condition will be applied.
**/
export function groupBy(column, groupOrder = "ASC", table = false) {

    if (_.isString(column)) {
        return table ? `${table}.${column} ${groupOrder}` : `${column} ${groupOrder}`;
    }

    if (_.isArray(column)) {
        const cols = column.map(col => groupBy(col, groupOrder, table));

        return `(${cols.join(", ")})`;
    }

    // Assumes the condition is an object
    const key = _.keys(column)[0];

    for(const val of _.values(column)) {
        const _val = val.toLowerCase();

        if ("asc" === _val || "desc" === _val) {
            return table ? `${table}.${key} ${val}`;
        }

        return table ? `${val}(${table}.${key})` : `${val}(${key})`;
    }
}

export function having(conds, format, table) {
    return whereClause(conds, format, table);
}