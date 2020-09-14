import * as _ from "underscore";
import Table from "./table";
import * as clause from "./clause";

export class Collection extends Table {
    /**
     Constructor

     @param {string} name
        The name of the collection. Also use as the name of the table collection in the database.
     @param {object} schema
        The object which defines the columns where the key property is the column's name.
        {
            @property {string} type
                The column type. Options are: Id|ForeignId|DateTime|String|Int|Object|Array|Boolean|
                Float|Enum
            @property {boolean} long
                For column's `String` type. Set to true if the value may contain more than 255
                characters.
            @property {boolean} required
                Whether the value of the column must be present.
            @property {boolean} index
                Whether the column will be indexed.
            @property {int} length
                Use in `String` and `Int` column type.
            @property {boolean} unique
                Whether the column's value must be unique.
            @property {array} enum
                Use to enumerate the values of the column's `Enum` type.
            @property {*|function} defaultValue
                The value to use if there's no value set during insert operation.
                When a function is set, it automatically consist of the following parameters:
                {
                    @param {*} value
                    @param {object} columns
                    @param {object} error
                        Use to set an error when needed.
                }
            @property {function} validate
                A function to execute to validate the column's value during insert and update operation.
                {
                    @param {*} value
                        The column's value
                    @param {object} columns
                        The list of columns.
                    @param {object} error
                        An error object container to use set an error.
                }
        }
     @param {object} config
        The database configuration to use on establishing connection and making transactions.
    **/
    constructor(name, schema, config = false, onCached = null, onClearCached = null) {
        super(name, schema, config);

        // Bind methods for convenience
        this.cachedData = Object.create(null);
        this.onCached = onCached;
        this.onClearCached = onClearCached;

        this.__maybeClearCached = this.__maybeClearCached.bind(this);
        this.__maybeReturnId = this.__maybeReturnId.bind(this);
        this.__maybeReturnIds = this.__maybeReturnIds.bind(this);
    }

    /**
     Insert row data in the database.

     @param {object} columns
        An object consisting the column/value where the property key is the column name and it's value
        is the actual value to insert in the database.
     @returns {Promise<[Error, *]>}
    **/
    async insert(columns) {
        const error = {},
            _columns = await this.__prepareColumns(columns, true, error);

        if (!_.isEmpty(error)) {
            return [_.setError(error.message, error.code)];
        }

        return this.exec(`INSERT INTO ?? SET ?`, [this.getName(), _columns])
            .then(this.__maybeClearCached)
            .then(this.__maybeReturnId);
    }

    /**
     Inserts multiple data into the database.

     @param {array<object>} columns
        A list of columns data object to insert into.
     @returns {Promise<[Error, *]>}
    **/
    async insertMany(columns = []) {
        const error = {},
            list = [];

        for(const column of columns) {
            if (!_.isEmpty(error)) {
                return [_.setError(error.message, error.code)];
            }

            const _column = await this.__prepareColumns(column, this.schema, true, error, this);

            list.push(_column);
        }

        const keys = Object.keys(list[0]),
            values = list.map(Object.values);

        return this.exec(`INSERT INTO ?? (??) VALUES ?`, [this.getName(), keys, values])
            .then(this.__maybeClearCached)
            .then(this.__maybeReturnIds);
    }

    /**
     Updates data in the database base on the given condition.

     @param {object} columns
        An object containing the updated data.
     @param {object} conditions
        The conditions that must be met for an update to take place.
     @returns {Promise<[Error, Boolean]>}
    **/
    async update(columns, conditions = {}) {
        const error = {},
            _columns = await this.__prepareColumns(columns, false, error);

        if (!_.isEmpty(error)) {
            return [_.setError(error.message, error.code)];
        }

        let sql = `UPDATE ?? SET ?`,
            table = this.getName(),
            format = [table, _columns];

        // Get conditions
        sql += this.__getConditions(conditions, format);

        return this.exec(sql, format)
            .then(this.__maybeClearCached)
            .then(this.__returnTrue);
    }

    /**
     Removes data in the database.

     @param {object} conditions
        The conditions to met prior to deleting the data.
     @returns {Promise<[Error, Boolean]>}
    **/
    delete(conditions = {}) {
        let sql = `DELETE FROM ??`,
            table = this.getName(),
            format = [table];

        // Get conditions
        sql += this.__getConditions(conditions, format);

        return this.exec(sql, format)
            .then(this.__maybeClearCached)
            .then(this.__returnTrue);
    }

    /**
     Find data in the database.

     @param {object} conditions
        The conditions to met prior to retrieving the datas.

     @returns {Promise<[Error, Array<*>]>}
    **/
    async find(conditions) {
        conditions = conditions || {};

        const cachedKey = this.__createCachedKey(conditions),
            cached = await this.__getCached(cachedKey);

        if (!_.isEmpty(cached)) {
            //return [null, cached];
        }

        const {columns, where, page, perPage} = conditions;

        let table = this.getName(),
            _columns = this.__prepareColumnsForQuery(columns),
            sql = `SELECT ${_columns} FROM ??`,
            format = [table];

        // Add conditions
        sql += this.__getConditions(conditions, format);

        return this.exec(sql, format).then(res => this.__cached(cachedKey, res));
    }

    /**
     Returns a single row data from the database.

     @param {string} columns
     @param {object} where
        The set of conditions to met prior to retrieving the data.
     @returns {Promise<[Error, Object]>}
    **/
    findOne(columns, where = {}) {
        return this.find({columns, where}).then(res => this.__returnOne(res));
    }

    /**
     Get the column value from the database.

     @param {string} column
        The name of the table's column to get the data from.
     @param {object} where
        The set of conditions to met prior to retrieving the column value.
     @returns {Promise<[Error, *]>}
    **/
    getValue(column, where = {}) {
        return this.findOne(column, where).then(res => this.__returnValue(res, column));
    }

    /**
     @private
    **/
    __createCachedKey(...obj) {
        const key = _.extend.call(null, obj);

        return _.unserialize(key);
    }

    /**
     @private
    **/
    __getCached(key) {
        return this.cachedData[key]||null;
    }

    /**
     @private
    **/
    __cached(key, [err, results]) {
        if (results && !_.isEmpty(results)) {
            const _results = this.__prepareColumnsForDisplay(results);

            _.define(this.cachedData, key, _results);

            return [null, _results];
        }

        return [err, results];
    }

    /**
     @private
    **/
    __clearCached() {
        this.cachedData = Object.create(null);
    }

    /**
     @private
     @callback
    **/
    __maybeClearCached([err, result]) {
        if (result) {
            this.__clearCached();
        }

        return [err, result];
    }

    /**
     Helper method to apply the different conditions set in a query.

     @private

     @param {object} conditions
        The conditions to met prior to deleting the data.
        {
            @property {object} where
            @property {string|object|array} orderBy
            @property {string} order
            @property {string|object|array<object>} groupBy
            @property {string} groupOrder
            @property {string|object|array<object>} having
            @property {int} page
            @property {int} perPage
        }
     @param {array} format
    **/
    __getConditions(conditions, format) {
        const cons = [],
            {where, orderBy, order, groupBy, groupOrder, having, page, perPage} = conditions,
            table = this.getName();

        if (where) {
            const _where = clause.whereClause(where, format, table);

            cons.push(` WHERE ${_where}`);
        }

        if (groupBy) {
            const _groupBy = clause.groupBy(groupBy, groupOrder, table);

            cons.push(` GROUP BY ${_groupBy}`);

            if (having) {
                const _having = clause.having(having, format, table);

                cons.push(` HAVING ${_having}`);
            }
        }

        if (orderBy) {
            const _order = clause.orderBy(orderBy, order, table);

            cons.push(` ORDER BY ${_order}`);
        }

        if (perPage) {
            cons.push(clause.limit(page, perPage));
        }

        return cons.join("");
    }

    /**
     @private
     @callback
    **/
    async __prepareColumns(columns, isInsert = false, error) {
        let _columns = {},
            schema = this.schema;

        for(const key of Object.keys(schema)) {
            let def = schema[key],
                value = columns[key];

            // Ignore default types unless there's a value.
            if (_.contains(["Id", "DateTime", "Timestamp"], def.type)) {
                if (value) {
                    _columns[key] = value;
                }

                continue;
            }

            if (isInsert && _.isUndefined(value)) {
                // Get default value
                if (def.defaultValue) {
                    if (_.isFunction(def.defaultValue)) {
                        value = await def.defaultValue.call(null, value, columns, error);
                    } else {
                        value = def.defaultValue;
                    }
                }

                if (_.isUndefined(value)) {
                    if (def.required) {
                        error.message = "Missing required value!";
                        error.code = `missing_${key}`;

                        return _columns;
                    }
                    
                    continue;
                }
            }

            if (!isInsert && _.isUndefined(value)) {
                continue;
            }

            if (def.validate) {
                const context = isInsert ? "insert" : "update";

                value = await def.validate.call(null, value, columns, context, error);

                if (!_.isEmpty(error)) {
                    return _columns;
                }
            }

            if (_.isObject(value) || _.isArray(value)) {
                value = _.serialize(value);
            }

            _columns[key] = value;
        }

        return _columns;
    }

    /**
     @private
    **/
    __maybeReturnId([err, result]) {
        if (err) {
            return [err];
        }

        // Clear caches
        this.__clearCached();

        return [null, result.insertId||result.affectedRows > 0];
    }

    /**
     @private
    **/
    __maybeReturnIds([err, results]) {
        if (err) {
            return [err];
        }

        this.__clearCached();

        // Get the last inserted id
        let lastId = results.insertId,
            ids = [];

        for (let i = 0; i < results.affectedRows; i++) {
            ids.push(i+lastId);
        }

        return [null, ids];
    }

    /**
     @private
    **/
    __prepareColumnsForQuery(columns) {
        let table = this.getName();

        if (!columns) {
            return `${table}.*`;
        }

        if (_.isArray(columns)) {
            return columns.map( col => this.__prepareColumnsForQuery(col)).join(" ");
        }

        // TODO: column object
        if (_.isObject(columns)) {
            const {$fn, column, as} = columns;

            let _column = column;
            if ($fn) {
                _column = `${$fn}(${column})`;
            }

            if (as) {
                _column += ` AS ${as}`;
            }

            return _column;
        }

        return `${table}.${columns}`;
    }

    /**
     @private
    **/
    __prepareColumnsForDisplay(columns) {
        
        if (_.isArray(columns)) {
            return columns.map(col => this.__prepareColumnsForDisplay(col));
        }

        const schema = this.schema;

        for(const column of Object.keys(columns)) {
            const def = schema[column];

            if (_.isUndefined(def)) {
                continue;
            }

            if (_.contains(["Object", "Array"], def.type)) {
                columns[column] = _.unserialize(columns[column]);
            }
        }

        return columns;
    }

    /**
     @private
     @callback
    **/
    __returnOne([err, results]) {
        if (results && results.length) {
            return [null, _.first(results)];
        }

        return [err];
    }

    /**
     @private
     @callback
    **/
    __returnValue([err, result], column) {
        if (err) {
            return [err];
        }

        if (_.isObject(column)) {
            if (column.as) {
                return [null, result[column.as]];
            }

            return result[column.column];
        }

        return [null, result[column]];
    }
}