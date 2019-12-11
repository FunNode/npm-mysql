/* eslint-env mocha */
const chai = require('chai');
const { expect } = chai;
const sinon = require('sinon');
const proxyquire = require('proxyquire');

chai.use(require('sinon-chai'));
chai.use(require('chai-as-promised'));

describe('Database', function () {
  let sandbox;
  let on;
  let createConnection;
  let mysqlLib;
  let Database;
  let database;

  function inject () {
    Database = proxyquire('../index', {
      'promise-mysql': mysqlLib,
    });
  }

  beforeEach(function () {
    sandbox = sinon.createSandbox();
    on = sandbox.stub();
  });
  
  afterEach(function () {
    sandbox.restore();
  });

  describe('with single host', function () {
    let destroy;
    let query;
    let config = { host: 'both' };
    
    beforeEach(function () {
      destroy = sandbox.stub();
      query = sandbox.stub().resolves({ result: 'result' });
      createConnection = sandbox.stub().resolves({
        on,
        destroy,
        query,
        connection: { threadId: 'threadId' },
      });
      mysqlLib = { createConnection };
      inject();
      database = new Database(config);
    });

    it('constructs', function () {
      expect(database.IN).to.not.be.undefined;
      expect(database.IN.config).to.eql(config);
      expect(database.OUT).to.not.be.undefined;
      expect(database.OUT.config).to.eql(config);
      expect(database.OUT).to.eql(database.IN);
    });

    it('connects', async function () {
      await database.connect();
      expect(createConnection).to.have.been.calledOnce;
      expect(createConnection.args[0][0]).to.eql({ host: 'both', reconnect: false });
      expect(on).to.have.been.calledOnce;
    });

    it('reconnects on connection lost error', async function () {
      await database.connect();
      const errorCallback = on.args[0][1];
      await errorCallback({ code: 'PROTOCOL_CONNECTION_LOST' });
      expect(createConnection).to.have.been.calledTwice;
      expect(on).to.have.been.calledTwice;
      expect(destroy).to.have.been.calledOnce;
    });

    it('reconnects on fatal error', async function () {
      await database.connect();
      const errorCallback = on.args[0][1];
      await errorCallback({ fatal: true });
      expect(createConnection).to.have.been.calledTwice;
      expect(on).to.have.been.calledTwice;
      expect(destroy).to.have.been.calledOnce;
    });

    it('does not reconnect on unknown errors', async function () {
      await database.connect();
      const errorCallback = on.args[0][1];
      await errorCallback({ error: 'error' })
        .then(() => expect.fail())
        .catch((err) => expect(err).to.eql({ error: 'error' }));
      expect(createConnection).to.have.been.calledOnce;
      expect(on).to.have.been.calledOnce;
      expect(destroy).to.not.have.been.called;
    });

    it('disconnects', async function () {
      await database.connect();
      database.disconnect();
      expect(destroy).to.have.been.calledOnce;
    });
    
    it('skips destroy if not connected', function () {
      database.disconnect();
      expect(destroy).to.not.have.been.called;
    });

    it('queries select', async function () {
      await database.connect();
      const queryStr = 'SELECT';
      const res = await database.query(queryStr);
      expect(res).to.eql({ result: 'result' });
      expect(query).to.have.been.calledOnce;
      expect(query.args[0][0]).to.eql(queryStr);
    });

    it('queries insert', async function () {
      await database.connect();
      const queryStr = 'INSERT';
      const res = await database.query(queryStr);
      expect(res).to.eql({ result: 'result' });
      expect(query).to.have.been.calledOnce;
      expect(query.args[0][0]).to.eql(queryStr);
    });
  });

  describe('with 2 hosts', function () {
    let destroyIn;
    let destroyOut;
    let queryIn;
    let queryOut;
    let configIn = { host: 'in' };
    let configOut = { host: 'out' };

    beforeEach(function() {
      destroyIn = sandbox.stub();
      destroyOut = sandbox.stub();
      queryIn = sandbox.stub().resolves({ result: 'result' });
      queryOut = sandbox.stub().resolves({ result: 'result' });
      createConnection = sandbox.stub();
      createConnection.onCall(0).resolves({
        on,
        destroy: destroyIn,
        query: queryIn,
        connection: { threadId: 'threadId' },
      });
      createConnection.onCall(1).resolves({
        on,
        destroy: destroyOut,
        query: queryOut,
        connection: { threadId: 'threadId' },
      });
      mysqlLib = { createConnection };
      inject();
      database = new Database(configIn, configOut);
    });

    it('constructs', function () {
      expect(database.IN).to.not.be.undefined;
      expect(database.IN.config).to.eql(configIn);
      expect(database.OUT).to.not.be.undefined;
      expect(database.OUT.config).to.eql(configOut);
      expect(database.OUT).to.not.eql(database.IN);
    });

    it('connects', async function () {
      await database.connect();
      expect(createConnection).to.have.been.calledTwice;
      expect(createConnection.args[0][0]).to.eql({ host: 'in', reconnect: false });
      expect(createConnection.args[1][0]).to.eql({ host: 'out', reconnect: false });
    });

    it('disconnects', async function () {
      await database.connect();
      database.disconnect();
      expect(destroyIn).to.have.been.calledOnce;
      expect(destroyOut).to.have.been.calledOnce;
    });
    
    it('queries select', async function () {
      await database.connect();
      const queryStr = 'SELECT';
      await database.query(queryStr);
      expect(queryIn).to.have.been.calledOnce;
      expect(queryIn.args[0][0]).to.eql(queryStr);
      expect(queryOut).to.not.have.been.called;
    });

    it('queries update', async function () {
      await database.connect();
      const queryStr = 'INSERT';
      await database.query(queryStr);
      expect(queryOut).to.have.been.calledOnce;
      expect(queryOut.args[0][0]).to.eql(queryStr);
      expect(queryIn).to.not.have.been.called;
    });

    it('queries update with object', async function () {
      await database.connect();
      const queryObj = { sql: 'INSERT' };
      await database.query(queryObj);
      expect(queryOut).to.have.been.calledOnce;
      expect(queryOut.args[0][0]).to.eql(queryObj);
      expect(queryIn).to.not.have.been.called;
    });
  });

  describe('with unreachable host', function () {
    let config = { host: 'in' };
    
    beforeEach(function () {
      createConnection = sandbox.stub().rejects({ error: 'error' });
      mysqlLib = { createConnection };
      inject();
      database = new Database(config);
      database.IN.error_timeout = 0;
    });
  
    it('retries connecting and gives up', async function () {
      await database.connect()
        .then(() => expect.fail())
        .catch((err) => expect(err).to.eql({ error: 'error' }));
      expect(createConnection.callCount).to.eql(11);
    });
  });

  describe('with difficult to reach host', function () {
    let config = { host: 'in' };
    
    beforeEach(function () {
      createConnection = sandbox.stub();
      (new Array(10)).fill(0).forEach((_, i) => createConnection.onCall(i).rejects({ error: 'error' }));
      createConnection.onCall(10).resolves({
        on,
        connection: { threadId: 'threadId' },
      });
      mysqlLib = { createConnection };
      inject();
      database = new Database(config);
      database.IN.error_timeout = 0;
    });
  
    it('retries connecting', async function () {
      await database.connect();
      expect(createConnection.callCount).to.eql(11);
    });
  });

  describe('with host unresponsive to queries', function () {
    let destroy;
    let query;
    let config = { host: 'in' };
    
    beforeEach(function () {
      destroy = sandbox.stub();
      query = sandbox.stub().rejects({ fatal: true, error: 'error' });
      createConnection = sandbox.stub().resolves({
        on,
        destroy,
        query,
        connection: { threadId: 'threadId' },
      });
      mysqlLib = { createConnection };
      inject();
      database = new Database(config);
      database.IN.error_timeout = 0;
    });

    it('retries and gives up', async function () {
      await database.connect();
      const queryStr = 'SELECT';
      await database.query(queryStr)
        .then(() => expect.fail())
        .catch(err => expect(err).to.eql({ fatal: true, error: 'error' }));
      expect(query.callCount).to.eql(11);
      expect(createConnection.callCount).to.eql(11);
      expect(destroy.callCount).to.eql(10);
    });
  });

  describe('with difficult to query host', function () {
    let destroy;
    let query;
    let config = { host: 'in' };
    
    beforeEach(function () {
      destroy = sandbox.stub();
      query = sandbox.stub();
      (new Array(10)).fill(0).forEach((_, i) => query.onCall(i).rejects({ fatal: true, error: 'error' }));
      query.onCall(10).resolves({ result: 'result' });
      createConnection = sandbox.stub().resolves({
        on,
        destroy,
        query,
        connection: { threadId: 'threadId' },
      });
      mysqlLib = { createConnection };
      inject();
      database = new Database(config);
      database.IN.error_timeout = 0;
    });

    it('retries querying', async function () {
      await database.connect();
      const queryStr = 'SELECT';
      const res = await database.query(queryStr);
      expect(res).to.eql({ result: 'result' });
      expect(query.callCount).to.eql(11);
    });
  });

  describe('with non fatal query error', function () {
    let query;
    let config = { host: 'in' };
    
    beforeEach(function () {
      query = sandbox.stub().rejects({ error: 'error' });
      createConnection = sandbox.stub().resolves({
        on,
        query,
        connection: { threadId: 'threadId' },
      });
      mysqlLib = { createConnection };
      inject();
      database = new Database(config);
      database.IN.error_timeout = 0;
    });

    it('gives up without retrying', async function () {
      await database.connect();
      const queryStr = 'SELECT';
      await database.query(queryStr)
        .then(() => expect.fail())
        .catch(err => expect(err).to.eql({ error: 'error' }));
      expect(query.callCount).to.eql(1);
    });
  });
});
