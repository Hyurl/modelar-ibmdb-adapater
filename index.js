const { Pool } = require("ibm_db");
const { Adapter } = require("modelar");
const Pools = {};

class IbmdbAdapter extends Adapter {
    constructor() {
        super();
        this.backquote = "\"";
    }

    /** Methods for DB */

    connect(db) {
        var { database, host, port, user, password, max } = db._config;
        var constr = `DATABASE=${database};HOSTNAME=${host};PORT=${port};PROTOCOL=TCPIP;UID=${user};PWD=${password}`;
        if (Pools[db._dsn] === undefined) {
            Pools[db._dsn] = new Pool();
            Pools[db._dsn].setMaxPoolSize(max);
        }
        return new Promise((resolve, reject) => {
            Pools[db._dsn].open(constr, (err, connection) => {
                if (err) {
                    reject(err);
                } else {
                    this.connection = connection;
                    resolve(db);
                }
            });
        });
    }

    query(db, sql, bindings) {
        var affectCommands = ["insert", "update", "delete"],
            affected = false;
        return new Promise((resolve, reject) => {
            if (affectCommands.includes(db._command)) {
                sql = `select count(*) as COUNT from new table (${sql})`;
                affected = true;
            }
            this.connection.query(sql, bindings, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    if (affected) {
                        db.affectedRows = rows[0].COUNT;
                    } else {
                        db._data = rows;
                        db.affectedRows = rows.length;
                    }
                    if (db._command === "insert") {
                        this.connection.query("select identity_val_local() from SYSIBM.SYSDUMMY1", [], (err, rows) => {
                            if (err) {
                                reject(err);
                            } else {
                                db.insertId = parseInt(rows[0][1]);
                                resolve(db);
                            }
                        });
                    } else {
                        resolve(db);
                    }
                }
            });
        });
    }

    transaction(db, callback = null) {
        var promise = new Promise((resolve, reject) => {
            this.connection.beginTransaction(err => {
                if (err) {
                    reject(err);
                } else {
                    resolve(db)
                }
            });
        });
        if (typeof callback == "function") {
            return promise.then(db => {
                let res = callback.call(db, db);
                if (res.then instanceof Function) {
                    return res.then(() => db);
                } else {
                    return db;
                }
            }).then(db => {
                return this.commit(db);
            }).catch(err => {
                return this.rollback(db).then(db => {
                    throw err;
                });
            });
        } else {
            return promise;
        }
    }

    commit(db) {
        return new Promise((resolve, reject) => {
            this.connection.commitTransaction(err => {
                if (err) {
                    reject(err);
                } else {
                    resolve(db);
                }
            });
        });
    }

    rollback(db) {
        return new Promise((resolve, reject) => {
            this.connection.rollbackTransaction(err => {
                if (err) {
                    reject(err);
                } else {
                    resolve(db);
                }
            });
        });
    }

    release() {
        this.close();
        this.connection = null;
    }

    close() {
        if (this.connection)
            this.connection.close();
    }

    static close() {
        for (let i in Pools) {
            Pools[i].close();
            delete Pools[i];
        }
    }

    /** Methods for Table */

    getDDL(table) {
        var numbers = ["int", "integer"],
            columns = [],
            foreigns = [],
            primary,
            autoIncrement,
            sql;

        for (let field of table._fields) {
            if (field.primary && field.autoIncrement) {
                if (!numbers.includes(field.type.toLowerCase())) {
                    field.type = "int";
                }
                autoIncrement = ` generated always as identity (start with ${field.autoIncrement[0]}, increment by ${field.autoIncrement[1]})`;
            } else {
                autoIncrement = null;
            }
            if (field.length instanceof Array) {
                field.type += "(" + field.length.join(",") + ")";
            } else if (field.length) {
                field.type += "(" + field.length + ")";
            }

            let column = table.backquote(field.name) + " " + field.type;

            if (field.primary)
                primary = field.name;
            if (field.default === null)
                column += " default null";
            else if (field.default !== undefined)
                column += " default " + table.quote(field.default);
            if (field.notNull)
                column += " not null";
            if (field.unsigned)
                column += " unsigned";
            if (field.unique)
                column += " unique";
            if (field.comment)
                column += " comment " + table.quote(field.comment);
            if (autoIncrement)
                column += autoIncrement;
            if (field.foreignKey.table) {
                let foreign = `foreign key (${table.backquote(field.name)})` +
                    " references " + table.backquote(field.foreignKey.table) +
                    " (" + table.backquote(field.foreignKey.field) + ")" +
                    " on delete " + field.foreignKey.onDelete +
                    " on update " + field.foreignKey.onUpdate;
                foreigns.push(foreign);
            };
            columns.push(column);
        }

        sql = "create table " + table.backquote(table._table) +
            " (\n\t" + columns.join(",\n\t");

        if (primary)
            sql += ",\n\tprimary key(" + table.backquote(primary) + ")";

        if (foreigns.length)
            sql += ",\n\t" + foreigns.join(",\n\t");

        return sql + "\n)";
    }

    /** Methods for Query */

    limit(query, length, offset = 0) {
        if (offset === 0) {
            query._limit = length;
        } else {
            query._limit = [offset, length];
        }
        return query;
    }

    getSelectSQL(query) {
        var isCount = (/count\(distinct\s\S+\)/i).test(query._selects),
            orderBy = query._orderBy ? `order by ${query._orderBy}` : "",
            paginated = query._limit instanceof Array,
            sql = "select ";

        sql += (query._distinct && !isCount ? "distinct " : "") + `${query._selects}`;

        if (paginated)
            sql += `, row_number() over(${orderBy}) rn`;

        sql += " from " +
            (!query._join ? query.backquote(query._table) : "") +
            query._join +
            (query._where ? " where " + query._where : "");

        if (!paginated && orderBy)
            sql += ` ${orderBy}`;

        sql += (query._groupBy ? " group by " + query._groupBy : "") +
            (query._having ? "having " + query._having : "");

        if (query._limit) {
            if (paginated)
                sql = `select * from (${sql}) tmp where tmp.rn > ${query._limit[0]} and tmp.rn <= ${query._limit[0] + query._limit[1]}`;
            else
                sql += ` fetch first ${query._limit} rows only`;
        }

        if (query._union)
            sql += ` union ${query._union}`;
        return sql;
    }
}

module.exports = IbmdbAdapter;