/* eslint-disable brace-style, camelcase, semi */
/* eslint-env mocha */

var chai = require('chai');
var assert = chai.assert;
const config = {
  read: {
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'funnode',
    debug: false
  },
  write: {
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'funnode',
    debug: false
  }
};
describe('Database', function () {
  let database = new (require('./index.js'))(config.read, config.write);

  describe('Checking Database Connection', function () {
    it('should not return errors', function (done) {
      database.query('select version()', function (err, results, fields) {
        assert.notEqual(typeof results, 'undefined', err);
        done();
      });
    });
  });
});
