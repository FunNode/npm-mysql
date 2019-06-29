/* eslint-disable brace-style, camelcase, semi */
/* eslint-env mocha */

require('dotenv').config();

var chai = require('chai');
var assert = chai.assert;

describe('Database', function () {
  let database = new (require('./index.js'))(true);

  describe('Checking Database Connection', function () {
    it('should not return errors', function (done) {
      database.query('select version()', function (err, results, fields) {
        assert.notEqual(typeof results, 'undefined', err);
        done();
      });
    });
  });
});
