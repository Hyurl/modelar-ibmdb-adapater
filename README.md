# Modelar-Ibmdb-Adapter

**This is an adapter for [Modelar](https://github.com/hyurl/modelar) to** 
**connect DB2 database.**

## Install

```sh
npm install modelar-ibmdb-adapter --save
```

The above command will install the latest version for Modelar 3.0+, if you're 
using Modelar 2.X, use the following command instead:

```sh
npm install modelar-ibmdb-adapter --tag modelar2 --save
```

## How To Use

```javascript
const { DB } = require("modelar");
const { IbmdbAdapter } = require("modelar-ibmdb-adapter");

DB.setAdapter("ibmdb", IbmdbAdapter).init({
    type: "ibmdb",
    database: "SAMPLE",
    host: "127.0.0.1",
    port: 50000,
    user: "db2admin",
    password: "******"
});
```

## Warning

DB2 database transfers identifiers to UPPER-CASE by default, but with this 
adapter, they will keep the form of which they're defined.