import * as _ from "../utils";
import mysql from "mysql";

/**
 Establishes and executes database transactions.
**/
export class Connect {
    /**
     Constructor

     @param {object} config
        The database configuration options to use to establish and executes a transaction.
        For details as to what the options are, refer to: https://github.com/mysqljs/mysql
        {
            @property {string} prefix
                A unique string use to prefix the table collection's name.
        }
    **/
    constructor(config) {
        this.config = _.extend({
            host: 'localhost',
            port: 3306,
            connectionLimit: 50,
            dateStrings: true,
            supportBigNumbers: true,
            multipleStatements: true,
            timezone: 'UTC'
        }, config);

        this.error = false;
        this.isMulti = false;
        this.client = false;
    }

    getClient() {
        return mysql.createConnection(this.config);
    }

    /**
     Returns the unique prefix used when creating collection table.

     @returns {string}
    **/
    getPrefix() {
        return this.config.prefix;
    }

    /**
     Use when executing multiple transactions in a single open connection.

     @returns {Boolean}
    **/
    multi() {
        this.isMulti = true;
    }

    /**
     Verify the integrity of the given configuration options.

     @returns {Promise<[Error, Boolean]>}
    **/
    verify() {
        return new Promise(res => {
            let client = this.getClient();

            client.connect( err => {
                if (err) {
                    this.error = err;
                    this.client = null;

                    return res([err]);
                }

                client.end();

                return res([null, true]);
            });
        });
    }

    /**
     Executes transaction unto the database.

     @param {string} sql
     @param {object} options
     @returns {Promise<[Error, *]>}
    **/
    exec(sql, options) {
        if (!this.client) {
            this.client = this.getClient();
        }

        return new Promise( res => {
            this.client.query(sql, options, (err, results) => {
                if (!this.isMulti) {
                    this.end();
                }

                if (err) {
                    this.error = err;

                    return res([err]);
                }

                return res([null, results]);
            });
        });
    }

    /**
     Closes database connection.

     @returns {void}
    **/
    end() {
        if (this.client && this.client.end) {
            this.client.end();
        }

        this.error = false;
        this.client = false;
        this.isMulti = false;
    }
}