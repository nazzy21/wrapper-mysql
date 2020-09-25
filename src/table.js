import * as _ from "./utils";
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

        if (!_.isEmpty(options)) {
            for(const key of _.keys(options)) {
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
        An object schema which was previously used to create the table collection.
     @param {object} newSchema
        A new set of object which redefines how a table collection structure is. If omitted, will use the schema use to defined
        by the sub-class.
     @param {object} options
        Additional table options to define how a table structure is.
     @returns {Promise<[Error, Boolean]>}
    **/
    async alter(oldSchema, newSchema = {}, options = {}) {
        // Mark for multiple transactions
        this.multi();

        if (!newSchema || _.isEmpty(newSchema)) {
            newSchema = this.schema;
        }

        const format = [this.getName()],
            sql = [],
            newIndexes = [],
            dropIndexes = [],
            newContraint = [];

        for(const key of _.keys(oldSchema)) {
            const oldColumn = oldSchema[key];

            if (!newSchema[key]) {
    
                sql.push(`DROP COLUMN ??`);
                format.push(key);

                if (oldColumn.index) {
                    dropIndexes.push(key);
                }

                if (oldColumn.foreign) {
                    sql.push(`DROP FOREIGN KEY ${oldColumn.foreign.key}`);
                }

                continue;
            }

            if (!_.isEqual(oldColumn, newSchema[key])) {
                const _old = _.clone(oldColumn),
                    _new = _.clone(newSchema[key]);

                // Check index
                if (_old.index && !_new.index) {
                    // Remove index
                    dropIndexes.push(key);

                    delete _old.index;
                } else if (!_old.index && _new.index) {
                    newIndexes.push(key);

                    delete _new.index;
                }

                // Check foreign
                if (_old.foreign && !_new.foreign) {
                    // Drop contraint
                    sql.push(`DROP FOREIGN KEY ??`);
                    format.push(_old.foreign.key);

                    delete _old.foreign;
                } else if (!_old.foreign && _new.foreign) {
                    // Add contraint
                    sql.push("ADD " + this.__contraint(key, _new));

                    delete _new.foreign;
                } else if (_old.foreign && _new.foreign) {
                    // Check the key
                    if (_old.foreign.key !== _new.foreign.key) {
                        // Remove foreign key
                        sql.push(`DROP FOREIGN KEY ??`);
                        format.push(_old.foreign.key);

                        sql.push("ADD " + this.__contraint(key, _new));
                    }

                    delete _old.foreign;
                    delete _new.foreign;
                }

                if (_.isEqual(_.serialize(_old), _.serialize(_new))) {
                    continue;
                }

                const [newColumn, _indexes] = this.__mapColumnStructure(_.object([key], [_new]));

                // Add the column
                sql.push(`CHANGE COLUMN ?? ${newColumn.join(", ")}`);
                format.push(key);
            }
        }

        // Get new columns
        for(const key of _.keys(newSchema)) {
            if (oldSchema[key]) {
                continue;
            }

            const obj = _.object([key], [newSchema[key]]),
                [newColumn, _indexes] = this.__mapColumnStructure(obj);

            // Add columns
            sql.push(`ADD COLUMN ${newColumn.join(" ")}`);

            if (_indexes.length) {
                newIndexes.push(key);
            }
        }

        // Alter the table structure in the database
        let sqlString = `ALTER TABLE ?? ${sql.join(", ")}`;

        if (options && !_.isEmpty(options)) {
            const _options = [];

            for(const _key of _.keys(options)) {
                _options.push(`${_key}=${options[key]}`);
            }

            sqlString += _options.join(", ");
        }

        const [err] = await this.exec(sqlString, format);

        if (err) {
            this.end();

            return [err];
        }

        // Remove indexes if there's any
        if (dropIndexes.length) {
            for(const index of dropIndexes) {
                await this.exec(`DROP INDEX ?? ON ??`, [index, this.getName()]);
            }
        }

        // Add new index
        if (newIndexes.length) {
            await this.exec('CREATE INDEX ?? ON ??', [newIndexes, this.getName()]);
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
     Helper method to return with a boolean result.

     @private
     @callback
    **/
    __returnTrue([err, done]) {
        return [err, !!done];
    }

    /**
     @private
    **/
    __contraint(name, def) {
        const table = this.getPrefix() + def.foreign.name, 
            foreign = [`CONSTRAINT ${def.foreign.key}`, `FOREIGN KEY (${name})`, `REFERENCES ${table}(${def.foreign.column})`],
            ref = {
                cascade: "CASCADE",
                strict: "STRICT",
                null: "SET NULL",
                default: "SET DEFAULT"
            };

        if (def.foreign.onDelete) {
            foreign.push(`ON DELETE ${ref[def.foreign.onDelete.toLowerCase()]}`);
        }

        if (def.foreign.onUpdate) {
            foreign.push(`ON UPDATE ${ref[def.foreign.onUpdate.toLowerCase()]}`);
        }

       return foreign.join(" ");
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
            indexes = [],
            foreignColumns = [];

        for(const name of _.keys(schema)) {
            const def = schema[name];

            let column = [`\`${name}\``],
                isDate = false;

            switch(def.type) {
                case 'Id' :
                    column.push(`BIGINT(20) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT`);
                    indexes.push(name);
                    break;

                case 'String' :
                    if (!def.length) {
                        column.push(`LONGTEXT`);
                        break;
                    }

                    let strLength = def.length;
                    column.push(`VARCHAR(${strLength})`);
                    break;

                case 'ForeignId' :
                    column.push('BIGINT(20) UNSIGNED');
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

                case 'Date' :
                case 'DateTime' :
                case 'Timestamp' :
                    isDate = true;
                    column.push(def.type.toUpperCase());
                    break;

                case 'Float' :
                    const fLength = def.length||4;
                    column.push(`FLOAT(${fLength})`);
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

            if (def.foreign) {
                foreignColumns.push(this.__contraint(name, def));
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

        if (foreignColumns.length) {
            columns.push(foreignColumns.join(" "));
        }

        return [columns, indexes];
    }
}