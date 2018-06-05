# Modelar-Ibmdb-Adapter

**This is an adapter for [Modelar](https://github.com/hyurl/modelar) to** 
**connect IBM DB2/Informix database.**

## Install

```sh
npm install modelar-ibmdb-adapter --save
```

## How To Use

```javascript
const { DB } = require("modelar");
const { IbmdbAdapter } = require("modelar-ibmdb-adapter");

DB.setAdapter("ibmdb", IbmdbAdapter)

// then using the type 'ibmdb' in db.config
DB.init({
    type: "ibmdb",
    database: "SAMPLE",
    host: "127.0.0.1",
    port: 50000,
    user: "db2admin",
    password: "******"
});
```

## How To Test

Since none of the open Continuous Integration environments support IBM DB2, 
if you want to test this package, you need to do the following steps manually 
in your computer.

### Prepare

Before testing this package, you must have an IBM DB2 Database server 
installed in your machine, if you haven't, please visit the 
[Downloads](https://www-01.ibm.com/marketing/iwm/iwm/web/pickUrxNew.do?source=swg-db2expressc)
page for instructions, for a developer, the **Express-C** edition is fine.

If you're not very familiar with IBM DB2 Database, I suggest you do some 
research on it before doing the test and probably using it.

### Test

```sh
git clone https://github.com/hyurl/modelar-ibmdb-adapter
cd modelar-ibmdb-adapter
npm install
vim test/config/db.js # edit the configuration to connect your database server
npm run prepare # will create neccesary tables, once tables are created,
npm test # you can run test as many times as you want, even change node versions
```