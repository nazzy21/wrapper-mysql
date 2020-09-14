import {isEmpty, indexOf, isEqual} from "underscore";
import {Connect} from "./connect";

/**
 The class use to transact of the specified collection table.
**/
export default class Table extends Connect {
    /**
     Constructor

     @param {string} name
        The name of the collection table.
     @param {object} schema
        An object defining the schema of the table's columns.
     @param {object} config
        The object use to establish and verify the database transaction.
    **/
    constructor(name, schema, config) {
        super(config);

        this.name = name;
        this.schema = schema;

        // Bind the helper method for convenience
        this.__returnTrue = this.__returnTrue.bind(this);
    }

    /**
     Returns an object defining the table's columns.

     @returns {object}
    **/
    getSchema() {
        return this.schema;
    }

    /**
     Returns the name of the collection how it is written in the database. The table's collection name
     is prefix base on the prefix set in the configuration.

     @returns {string}
    **/
    getName() {
        return this.getPrefix() + this.name;
    }

    /**
     Creates table collection in the database if it does not exist.

     @params {object} options
        Additional options defining how a table must be created.
     @returns {Promise<[Error, Boolean]>}
    **/
    create(options = {}) {
        const [columns, indexes] = this.__mapColumnStructure(this.schema),
            _options = [];

        if (indexes && indexes.length) {
            columns.push('Index (`' + indexes.join('`,`') + '`)');
        }

        if (!isEmpty(options)) {
            for(const key in options) {
                if (!options.hasOwnProperty(key)) {
                    continue;
                }
                _options.push(`${key}=${options[key]}`);
            }
        }

        let sql = `CREATE TABLE IF NOT EXISTS ?? (` + columns.join(',') + `)${_options.join(' ')}`,
            format = [this.getName()];

        return this.exec(sql, format).then(this.__returnTrue);
    }

    /**
     Alter the table structure in the database.

     @param {object} oldSchema
        An object schema which previously used to create the table collection.
     @param {object} newSchema
        A new set of object which redefines how a table structure is. If omitted, will use the schema use to defined
        by the sub-class.
     @param {object} options
        Additional table options to define how a table structure is.
     @returns {Promise<[Error, Boolean]>}
    **/
    async alter(oldSchema, newSchema = {}, options = {}) {
        // Mark for multiple transactions
        this.multi();

        if (!newSchema || isEmpty(newSchema)) {
            newSchema = this.schema;
        }

        let [, indexes] = this.__mapColumnStructure(oldSchema),
            format = [this.getName()],
            sql = [],
            newColumns = {},
            updateColumns = {};

        for(const name in newSchema) {
            if (!newSchema.hasOwnProperty(name)) {
                continue;
            }

            const column = newSchema[name];

            if (!oldSchema[name]) {
                newColumns[name] = column;
                continue;
            }

            if (!isEqual(oldSchema[name], column)) {
                updateColumns[name] = column;
            }
        }

        let newIndexes = [],
            delIndexes = [];

        if (!isEmpty(newColumns)) {
            let [newCols, indexes] = this.__mapColumnStructure(newColumns);

            if (newCols) {
                sql.push('ADD ' + newCols.join(', ADD ') );
            }

            newIndexes = indexes;
        }

        for(const key in updateColumns) {
            if (!updateColumns.hasOwnProperty(key)) {
                continue;
            }

            const col = {};
            col[name] = updateColumns[key];

            const [def, index] = this.__mapColumnStructure(col);

            sql.push(`CHANGE COLUMN ?? ${def.join(" ")}`);
            format.push(name);

            if (isEmpty(index)) {
                continue;
            }

            const pos = indexOf(indexes, name);

            if (!pos) {
                newIndexes.push(name);
                continue;
            }

            delIndexes.push(name);
        }

        // Get deletable columns
        for(const key in oldSchema) {
            if (!oldSchema.hasOwnProperty(key)) {
                continue;
            }

            if (newSchema[key]) {
                continue;
            }

            sql.push(`DROP COLUMN ??`);
            format.push(key);

            if (indexOf(indexes, key) >= 0) {
                delIndexes.push(key);
            }
        }

        // Alter current collection table
        let [err] = await this.exec(`ALTER TABLE ?? ` + sql.join(', '), format);

        if (err) {
            this.end();

            return [err];
        }

        // Create new indexes
        if (newIndexes.length) {
            let [err2] = await this.exec('CREATE INDEX ?? ON ??', [newIndexes, this.getName()]);

            if (err2) {
                this.end();

                return [err2];
            }
        }

        // Remove indexes
        if (delIndexes.length) {
            for(const index of delIndexes) {
                await this.exec(`DROP INDEX ?? ON ??`, [index, this.getName()]);
            }
        }

        this.end();

        return [null, true];
    }

    /**
     Removes table collection in the database.

     @returns {Promise<[Error, Boolean]>}
    **/
    drop() {
        return this.exec('DROP TABLE ??', [this.getName()]).then(this.__returnTrue);
    }

    /**
     Copy table to a new table collection.

     @param {string} newName
     @returns {Promise<[Error, Boolean]>}
    **/
    clone(newName) {
        newName = this.transport.getPrefix() + newName.toLowerCase();

        return this.exec('CREATE TABLE IF NOT EXISTS ?? LIKE ??', [this.getName(), newName]).then(this.__returnTrue);
    }

    /**
     Helper method to return with a boolean result.

     @private
     @callback
    **/
    __returnTrue([err, done]) {
        return [err, !!done];
    }

    /**
     Helper method to map the column structure according to MySQL database column structure format.

     @private
     @callback

     @param {object} schema
        The schema to map into.
    **/
    __mapColumnStructure(schema) {
            const columns = [],
            indexes = [];

        for(const name in schema) {
            if (!schema.hasOwnProperty(name)) {
                continue;
            }

            const def = Object.create(schema[name]);

            let column = [`\`${name}\``],
                isDate = false;

            switch(def.type) {
                case 'Id' :
                    column.push(`BIGINT(20) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT`);
                    indexes.push(name);
                    break;

                case 'String' :
                    if (!def.length || def.long) {
                        column.push(`LONGTEXT`);
                        break;
                    }

                    let strLength = def.length;
                    column.push(`VARCHAR(${strLength})`);
                    break;

                case 'ForeignId' :
                    column.push('BIGINT(20)');
                    break;

                case 'Enum' :
                    let enums = '"' + def.enum.join('", "') + '"';
                    column.push(`ENUM (${enums})`);
                    break;

                case 'Int' :
                    let intLength = def.length || 11;

                    column.push(`INT(${intLength})`);
                    break;

                case 'Object' :
                case 'Array' :
                    column.push('LONGTEXT');
                    break;

                case 'Boolean' :
                    column.push('CHAR(1)');
                    break;

                case 'DateTime' :
                case 'Timestamp' :
                    isDate = true;
                    column.push(def.type.toUpperCase());
                    break;

                case 'Float' :
                    column.push(`FLOAT(4)`);
                    break;
            }

            if (def.required) {
                column.push('NOT NULL');
            }

            if (def.unique) {
                column.push('UNIQUE');
            }

            if (def.primary) {
                column.push('PRIMARY KEY');
            }

            if (isDate) {
                if (def.defaultValue) {
                    column.push(`DEFAULT CURRENT_TIMESTAMP`);
                }

                if (def.update) {
                    column.push('ON UPDATE CURRENT_TIMESTAMP');
                }
            }

            if (def.index) {
                indexes.push(name);
            }

            columns.push(column.join(' '));
        }

        return [columns, indexes];
    }
}