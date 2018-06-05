var DB = require("modelar").DB;
var IbmdbAdapter = require("../../").default;

module.exports = {
    type: "ibmdb",
    database: "SAMPLE",
    host: "127.0.0.1",
    port: 50000,
    user: "db2admin",
    password: "161301"
};

DB.setAdapter("ibmdb", IbmdbAdapter);
DB.init(module.exports);