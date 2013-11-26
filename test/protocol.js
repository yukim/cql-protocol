var test = require('tap').test,
    protocol = require('../lib/protocol');

test('test ser/de messages', function(t) {
  var serde = new protocol.NativeProtocol();

  // default protocol version should be 2
  t.equal(serde.protocolVersion, 2);

  serde.pipe(serde);

  t.test('messages.Error', function(t) {
    var message = new protocol.messages.Error(0x0000, 'Server error');
    t.equal(message.opcode, 0x00);
    t.equal(message.errorCode, 0x0000);
    t.equal(message.errorMessage, 'Server error');
    t.same(message.details, {});

    serde.once('message', function(received) {
      // verify message
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.equal(received.errorCode, message.errorCode);
      t.equal(received.errorMessage, message.errorMessage);
      t.same(received.details, message.details);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Error with details(Unavailable)', function(t) {
    var details = {
          consistencyLevel: protocol.CONSISTENCY_LEVEL.ONE,
          required: 3,
          alive: 1
        },
        message = new protocol.messages.Error(0x1000, 'Unavailable exception', details);
    t.equal(message.opcode, 0x00);
    t.equal(message.errorCode, 0x1000);
    t.equal(message.errorMessage, 'Unavailable exception');
    t.same(message.details, details);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.equal(received.errorCode, message.errorCode);
      t.equal(received.errorMessage, message.errorMessage);
      t.same(received.details, message.details);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Startup', function(t) {
    var message = new protocol.messages.Startup();
    t.equal(message.opcode, 0x01);
    t.same(message.options, {CQL_VERSION: '3.1.0'});

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.same(received.options, message.options);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Ready', function(t) {
    var message = new protocol.messages.Ready();
    t.equal(message.opcode, 0x02);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Authenticate', function(t) {
    var message = new protocol.messages.Authenticate();
    message.authenticator = 'dummy authenticator';

    t.equal(message.opcode, 0x03);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.equal(received.authenticator, message.authenticator);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Credentials', function(t) {
    var message = new protocol.messages.Credentials();
    message.credentials = {user: 'dummy'};
    t.equal(message.opcode, 0x04);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.same(received.credentials, message.credentials);
      t.end();
    });
    serde.sendMessage(message);
  });

  t.test('messages.Options', function(t) {
    var message = new protocol.messages.Options();
    t.equal(message.opcode, 0x05);
    t.equal(message.isRequest, true);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Supported', function(t) {
    var message = new protocol.messages.Supported();
    message.options = {CQL_VERSION: ['3.1.0']};
    t.equal(message.opcode, 0x06);
    t.equal(message.isRequest, false);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Query', function(t) {
    var message = new protocol.messages.Query();
    message.streamId = 99;
    message.enableTracing();
    message.query = 'SELECT * FROM table';
    t.equal(message.opcode, 0x07);
    t.equal(message.isRequest, true);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.equal(received.flags, message.flags);
      t.equal(received.streamId, message.streamId);
      t.equal(received.query, message.query);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Query with options', function(t) {
    var message = new protocol.messages.Query();
    message.query = 'SELECT * FROM table';
    message.options.pageSize = 5;
    message.options.pagingState = new Buffer('dummy paging state');

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.equal(received.query, message.query);
      t.equal(received.options.consistencyLevel, 1);
      t.equal(received.options.pageSize, 5);
      t.same(received.options.pagingState, new Buffer('dummy paging state'));
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Prepare', function(t) {
    var message = new protocol.messages.Prepare();
    message.query = 'SELECT * FROM table';
    t.equal(message.opcode, 0x09);
    t.equal(message.isRequest, true);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.equal(received.query, message.query);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Execute', function(t) {
    var id = new Buffer('some prepare statement id'),
        v = new Buffer('some binding value'),
        message = new protocol.messages.Execute(id);
    message.options.setValues(v);
    t.equal(message.opcode, 0x0A);
    t.equal(message.isRequest, true);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.same(received.id, message.id);
      t.same(received.options.values[0], message.options.values[0]);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Register', function(t) {
    var message = new protocol.messages.Register();
    message.events = ['TOPOLOGY_CHANGE', 'STATUS_CHANGE', 'SCHEMA_CHANGE'];
    t.equal(message.opcode, 0x0B);
    t.equal(message.isRequest, true);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.same(received.events, message.events);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Event', function(t) {
    var message = new protocol.messages.Event();
    message['event'] = {
      type: 'SCHEMA_CHANGE',
      typeOfChange: 'DROP',
      keyspace: 'test',
      table: 'test_cf'
    };
    message.streamId = -1;
    t.equal(message.opcode, 0x0C);
    t.equal(message.isRequest, false);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.same(received['event'], message['event']);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.Batch', function(t) {
    var message = new protocol.messages.Batch(0),
        i, j;
    t.equal(message.opcode, 0x0D);
    t.equal(message.type, 0);
    t.equal(message.queries.length, 0);
    t.equal(message.values.length, 0);
    t.equal(message.isRequest, true);

    message.add('SELECT * FROM table');
    message.add(new Buffer('dummy prepare statement id'), [new Buffer('dummy value')]);

    t.equal(message.queries.length, 2);
    t.equal(message.values.length, 2);

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      // since node-tap does not handle Buffers in an array...
      t.same(received.queries.length, received.values.length);
      t.same(received.queries.length, message.queries.length);
      t.same(received.values.length, message.values.length);
      for (i = 0; i < received.queries.length; i++) {
        t.same(received.queries[i], message.queries[i]);
        for (j = 0; j < received.values[i].length; j++) {
          t.same(received.values[i][j], message.values[i][j]);
        }
      }
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.AuthChallenge', function(t) {
    var message = new protocol.messages.AuthChallenge();
    t.equal(message.token.length, 0);
    t.equal(message.opcode, 0x0E);
    t.equal(message.isRequest, false);
    message.token = new Buffer('dummy challenge');

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.same(received.token, message.token);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.AuthResponse', function(t) {
    var message = new protocol.messages.AuthResponse();
    t.equal(message.token.length, 0);
    t.equal(message.opcode, 0x0F);
    t.equal(message.isRequest, true);
    message.token = new Buffer('dummy response');

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.same(received.token, message.token);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('messages.AuthSuccess', function(t) {
    var message = new protocol.messages.AuthSuccess();
    t.equal(message.token.length, 0);
    t.equal(message.opcode, 0x10);
    t.equal(message.isRequest, false);
    message.token = new Buffer('dummy success');

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.same(received.token, message.token);
      t.end();
    });

    serde.sendMessage(message);
  });

  t.test('mesasges.SplitChunk', function(t) {

    var message = new protocol.messages.Query();
    message.query = 'SELECT * FROM table';
    message.options.pageSize = 5;
    message.options.pagingState = new Buffer('dummy paging state');

    serde.once('message', function(received) {
      t.type(received, typeof message);
      t.equal(received.opcode, message.opcode);
      t.equal(received.query, message.query);
      t.equal(received.options.consistencyLevel, 1);
      t.equal(received.options.pageSize, 5);
      t.same(received.options.pagingState, new Buffer('dummy paging state'));
      t.end();
    });

    var header = new Buffer(8);
    header.writeUInt8(2, 0);
    header.writeUInt8(message.flags, 1);
    header.writeInt8(message.streamId, 2);
    header.writeUInt8(message.opcode, 3);

    var buffer = message.encode(2);

    header.writeUInt32BE(buffer.length, 4);

    // split head chunk
    serde.push(header.slice(0, 4));
    serde.push(header.slice(4));
    // split body chunk
    serde.push(buffer.slice(0, 10));
    serde.push(buffer.slice(10, 20));
    serde.push(buffer.slice(20));

  });

  t.test("teardown", function (t) {
    serde.shutdown();
    t.end();
  });
});

