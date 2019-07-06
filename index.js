/* eslint-disable brace-style, camelcase, semi */
/* global R5 */

module.exports = Database;

if (!global.R5) {
  global.R5 = {
    out: console
  }
}

let mysql = require('mysql');

// Constructor

function Database (read_config, write_config = false) {
  this.IN = new Host(read_config);

  if (write_config && read_config['host'] !== write_config['host']) {
    this.OUT = new Host(write_config);
  }
  else {
    this.OUT = this.IN;
  }
}

function Host (config) {
  this.error_retries = 0;
  this.error_timeout = 10000;
  this.config = config;
  connect(this);
}

// Public Methods

Database.prototype.query = function (query, callback) {
  let host = this.IN;
  if (
    (typeof query === 'string' && is_update(query)) ||
    (typeof query === 'object' && is_update(query.sql))
  ) {
    host = this.OUT;
  }
  host.query(query, callback);
};

Host.prototype = {
  query: function (query, callback) {
    let host = this;
    host.connection.query(query, function (err, results, fields) {
      if (err && err.fatal) {
        host.retry(query, callback);
      }
      else if (typeof callback === 'function') {
        if (err) { R5.out.error(`Query error: ${err}\n${query}\n`); }
        callback(err, results, fields);
      }
    });
  },

  retry: function (query, callback) {
    let host = this;
    setTimeout(function () {
      host.connection.destroy();
      connect(host);
      if (query) { host.query(query, callback); }
    }, host.error_timeout * (host.error_retries + 1));
  }
};

// Private Methods

function connect (host) {
  host.connection = mysql.createConnection(host.config);

  host.connection.connect(function (err) {
    if (err) {
      if (host.error_retries++ < 10) {
        R5.out.error(`connecting (retrying [${host.error_retries}]): ${err.code}`);
        host.retry();
        return;
      }
      R5.out.error(`connecting: ${err.stack}`);
      throw err;
    }
    R5.out.log(`Connected to MySQL (conn: ${host.connection.threadId})`);
    host.error_retries = 0;
  });

  host.connection.on('error', function (err) {
    R5.out.error((err.fatal === true ? '(FATAL) ' : '') + err);
    if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.fatal === true) {
      host.connection.destroy();
      connect(host);
    }
    else {
      throw err;
    }
  });
}

function is_update (str) {
  return str.substring(0, 6).toUpperCase() !== 'SELECT';
}
