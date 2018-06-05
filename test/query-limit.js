var assert = require("assert");
var Query = require("modelar").Query;

describe("Query.prototype.limit()", function () {
    describe("limit(length: number)", function () {
        it("should generate SQL with a limit clause", function () {
            var query = new Query().select("*").from("users").limit(10);
            assert.equal(query.getSelectSQL(), 'select * from "users" fetch first 10 rows only');
        });
    });

    describe("limit(length: number, offset: number)", function () {
        it("should generate SQL with a limit clause along with an offset", function () {
            var query = new Query().select("*").from("users").limit(10, 31);
            assert.equal(query.getSelectSQL(), 'select * from (select *, row_number() over() "_rn" from "users") tmp where tmp."_rn" > 31 and tmp."_rn" <= 41');
        });
    });
})