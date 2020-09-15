import * as _ from "./utils";
import Table from "./table";
import * as clause from "./clause";

export class Collection extends Table {
    /**
     Constructor

     @param {string} name
        The name of the collection. Also use as the name of the table collection in the database.
     @param {object} schema
        The object which defines the table collection's columns. The key property is the column's name
        while it's value is another object which sets how a column should be.
        {
            @property {string} type
                The column type. Options are: Id|Date|DateTime|Timestamp|String|Int|Object|Array|Boolean|
                Float|Enum|ForeignId

                If the column type is `Id`, it is created as unsigned BIG integer which incremented
                for every new data insertion.

                If the column type is `ForeignId`, it assumes an `Id` of type created on other table collection and
                is created as BIG  integer width `0` as default value.

                If the column type is `String` and no length specified, it automattically created as LONGTEXT
                type.

                The values for types `Object` and `Array` are automattically serialized when saving into the
                database and unserialize when retrieving the data.
            
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
            @property {object} foreign
                An object defining the foreign table collections information.
                {
                    @property {string} key
                        A unique constraint key use for reference.
                    @property {string} name
                        The name of the table collection, with prefix.
                    @property {string} column
                        The column name set in the foreign table collection.
                    @property {string} onDelete
                        The reference name to use when the parent forign table collection deleted the row.
                        Options are `cascade`, `strict`, `null` or `default`
                    @property {string} onUpdate
                        The reference name to use when the parent forign table collection row is updated.
                        Options are `cascade`, `strict`, `null` or `default`
                }
            @property {*|function} defaultValue
                The value to use if there's no value set during insert operation.
                If the set value is a callable function, the function takes the following parameters:
                {
                    @param {object} columns
                    @param {object} error
                        Use to set an error when needed.
                    @param {object<Collection>} collection
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
                    @param {object<Collection>} collection
                }
        }
     @param {object} config
        The database configuration to use on establishing connection and making transactions.
    **/
    constructor(name, schema, config = false, onCached = null, onClearCached = null) {
        super(name, schema, config);

        this.cachedData = Object.create(null);
        this.onCached = onCached;
        this.onClearCached = onClearCached;

        // Bind methods for convenience
        this.__filterResult = this.__filterResult.bind(this);
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
            _columns = await this.__prepareColumnsForInsert(columns, error);

        if (!_.isEmpty(error)) {
            return [_.setError(error.message, error.code)];
        }

        return this.exec(`INSERT INTO ?? SET ?`, [this.getName(), _columns])
            .then(this.__filterResult);
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

            const _column = await this.__prepareColumnsForInsert(column, this.schema, error);

            list.push(_column);
        }

        const keys = Object.keys(list[0]),
            values = list.map(Object.values);

        return this.exec(`INSERT INTO ?? (??) VALUES ?`, [this.getName(), keys, values])
            .then(this.__filterResult);
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
            _columns = await this.__prepareColumnsForUpdate(columns, error);

        if (!_.isEmpty(error)) {
            return [_.setError(error.message, error.code)];
        }

        let sql = `UPDATE ?? SET ?`,
            table = this.getName(),
            format = [table, _columns];

        // Get conditions
        sql += this.__getConditions(conditions, format);

        return this.exec(sql, format)
            .then(this.__filterResult);
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

        return this.exec(sql, format).then(this.__filterResult);
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
            return [null, cached];
        }

        const {columns} = conditions;

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
     @param {object} whereClause
        The set of conditions to met prior to retrieving the data.
     @returns {Promise<[Error, Object]>}
    **/
    findOne(columns, whereClause = {}) {
        return this.find({columns, whereClause}).then(res => this.__returnOne(res));
    }

    /**
     Get the column value from the database.

     @param {string} column
        The name of the table's column to get the data from.
     @param {object} whereClause
        The set of conditions to met prior to retrieving the column value.
     @returns {Promise<[Error, *]>}
    **/
    getValue(column, whereClause = {}) {
        return this.findOne(column, whereClause).then(res => this.__returnValue(res, column));
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

            // Save the results
            this.cachedData[key] = _results;

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
    **/
    async __prepareColumnsForInsert(columns, error) {
        const _columns = {},
            schema = this.schema;

        for(const key of _.keys(schema)) {
            let def = schema[key],
                value = columns[key];

            // Ignore auto generated values
            if (_.contains(["Id", "Date", "DateTime", "Timestamp"], def.type)) {
                continue;
            }

            // Maybe set the default value if the value is missing
            if (!value || _.isUndefined(value)) {
                if (def.defaultValue) {
                    value = def.defaultValue;

                    if (_.isFunction(value)) {
                        value = value.call(null, columns, error, this);
                    }
                }

                if (def.required && _.isEmpty(value)) {
                    error.message = `Missing required value for ${key}!`;
                    error.code = 'missing_value';

                    return _columns; // No need to go further is an error occured
                }
            }

            if ("Boolean" === def.type) {
                value = !!value ? 1 : 0;
            }

            // Check validation
            if (def.validate) {
                value = await def.validate.call(null, value, columns, "insert", error, this);

                if (error && error.message) {
                    return _columns;
                }
            }

            // Maybe serialize?
            if (_.contains(["Object", "Array"], def.type)) {
                value = _.serialize(value);
            }

            _columns[key] = value;
        }

        return _columns;
    }

    /**
     @private
    **/
    async __prepareColumnsForUpdate(columns, error) {
        let _columns = _.clone(columns),
            schema = this.schema;

        for(const key of _.keys(schema)) {
            let def = schema[key],
                value = columns[key];

            // Validate the given value
            if (def.validate) {
                value = await def.validate.call(null, value, columns, "update", error);

                if (error && error.message) {
                    return _columns;
                }
            }

            // Maybe serialize
            if (_.contains(["Object", "Array"], def.type)) {
                value = _.serialize(value);
            }

            _columns[key] = value;
        }

        return _columns;
    }

    __filterResult([err, result]) {
        if (err) {
            return [err];
        }

        // Clear caches
        this.__clearCached();

        // Return an IDs if existed
        if (result.insertId) {
            if (result.affectedRows > 1) {
                // Get the last inserted id
                let lastId = result.insertId,
                    ids = [];

                for (let i = 0; i < result.affectedRows; i++) {
                    ids.push(i+lastId);
                }

                return [null, ids];
            }

            return [null, result.insertId];
        }

        if (result.affectedRows) {
            return [null, !!result.affectedRows];
        }

        return [null, result];
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

        for(const column of _.keys(columns)) {
            const def = schema[column],
                value = columns[column];

            if (_.isUndefined(def)) {
                continue;
            }

            switch(def.type) {
                case "Boolean" :
                    columns[column] = !!parseInt(value);
                    break;

                case "Object" :
                    columns[column] = !_.isEmpty(value) ? _.unserialize(value) : {};
                    break;

                case "Array" :
                    columns[column] = !_.isEmpty(value) ? _.unserialize(value) : [];
                    break;
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