/* global R5 */

module.exports = Database;

if (!global.R5) {
  global.R5 = {
    out: console
  };
}

const mysql = require('mysql2/promise');

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
  this.connect_retries = 0;
  this.query_retries = 0;
  this.error_timeout = 10000;
  this.config = config;
}

// Public Methods

Database.prototype = {
  connect: async function () {
    await this.IN.connect();
    if (this.OUT !== this.IN) {
      await this.OUT.connect();
    }
  },

  disconnect: function () {
    this.IN.destroy();
    if (this.OUT !== this.IN) {
      this.OUT.destroy();
    }
  },

  query: async function (query) {
    let host = this.IN;
    if (
      (typeof query === 'string' && is_update(query)) ||
      (typeof query === 'object' && is_update(query.sql))
    ) {
      host = this.OUT;
    }
    return host.query(query);
  },
};

Host.prototype = {
  connect: async function () {
    const host = this;
    try {
      this.connection = await mysql.createConnection(host.config);
    }
    catch (err) {
      if (host.connect_retries++ < 10) {
        R5.out.error(`MySQL connecting (retrying [${host.connect_retries}]): ${err.code}`);
        return host.retry();
      }
      R5.out.error(`MySQL connecting: ${err.stack}`);
      throw err;
    }
    R5.out.log(`MySQL connected (conn: ${host.connection.connection.threadId })`);
    this.connect_retries = 0;
    host.connection.on('error', async function (err) {
      R5.out.error((err.fatal === true ? '(FATAL) ' : '') + err);
      if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.fatal === true) {
        host.destroy();
        return host.connect();
      }
      else {
        throw err;
      }
    });  
  },
  
  destroy: function () {
    const host = this;
    if ((host.connection || {}).destroy) {
      host.connection.destroy();
    }
  },

  query: async function (query) {
    const host = this;
    try {
      let res;
      if (typeof query === 'object') {
        res = await host.connection.query(query.sql, query.values);
      }
      else {
        res = await host.connection.query(query);
      }
      this.query_retries = 0;
      return res;
    }
    catch (err) {
      if (err.fatal && host.query_retries++ < 10) {
        return host.retry(query);
      }
      R5.out.error(`MySQL query error: ${err}\n${query}\n`);
      throw err;
    }
  },

  retry: async function (query) {
    const host = this;
    host.destroy();
    await delay(host.error_timeout * (host.connect_retries + host.query_retries + 1));
    await host.connect();
    if (query) {
      return host.query(query);
    }
  },
};

// Private Methods

function is_update (str) {
  return str.substring(0, 6).toUpperCase() !== 'SELECT';
}

function delay (ms) {
  return new Promise((res) => setTimeout(res, ms));
}
