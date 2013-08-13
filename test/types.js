var test = require('tap').test,
    types = require('../lib/types');

test('test ser/de', function(t) {
  t.test('bigint', function(t) {
    var buf = types.bigint.serialize(100);
    t.equal(buf.length, 8);
    t.equal(types.bigint.deserialize(buf), '100');

    // serialize java's Long.MAX_VALUE
    buf = types.bigint.serialize('9223372036854775807');
    t.equal(buf.length, 8);
    t.equal(types.bigint.deserialize(buf), '9223372036854775807');

    // serialize java's Long.MIN_VALUE
    buf = types.bigint.serialize('-9223372036854775808');
    t.equal(types.bigint.deserialize(buf), '-9223372036854775808');

    // null should be null
    t.equal(types.bigint.deserialize(null), null, 'deserializing null should be null');

    t.end();
  });

  t.test('blob', function(t) {
    var blob = new Buffer('abc'),
        buf = types.blob.serialize(blob),
        des;
    t.notEqual(buf, blob, 'two Buffers are not equal, ...');
    t.same(buf, blob, '...but same');
    des = types.blob.deserialize(buf);

    t.equal(buf, des, 'deserialize returns the same object');
    t.equal(types.blob.deserialize(null), null, 'deserializing null should be null');
    t.end();
  });

  t.test('boolean', function(t) {
    var buf = types['boolean'].serialize(true);
    t.equal(buf.length, 1);
    t.equal(types['boolean'].deserialize(buf), true);
    buf = types['boolean'].serialize(false);
    t.equal(types['boolean'].deserialize(buf), false);

    t.equal(types['boolean'].deserialize(null), false, 'deserializing null should be false');

    t.end();
  });

  t.test('float', function(t) {
    var buf = types['float'].serialize(64.0);
    t.equal(buf.length, 4);
    t.equal(types['float'].deserialize(buf), 64.0);

    buf = types['float'].serialize(-2.5);
    t.equal(types['float'].deserialize(buf), -2.5);

    t.equal(types['float'].deserialize(null), null, 'deserializing null should be null');

    t.end();
  });

  t.test('int', function(t) {
    var buf = types['int'].serialize(64);
    t.equal(buf.length, 4);
    t.equal(types['int'].deserialize(buf), 64);

    var buf = types['int'].serialize(-2);
    t.equal(types['int'].deserialize(buf), -2);

    t.equal(types['int'].deserialize(null), null, 'deserializing null should be null');

    t.end();
  });

  t.test('timestamp', function(t) {
    var d = new Date(),
        buf = types.timestamp.serialize(d),
        s = types.timestamp.deserialize(buf);

    t.same(d, s);

    t.throws(function() {
      types.timestamp.serialize('foo');
    });
    t.end();
  });

  t.test('inet', function(t) {
    // IPv4
    var v4 = '10.10.10.10',
        v6 = '2001:db8:1234::1',
        buf = types.inet.serialize(v4),
        addr = types.inet.deserialize(buf);
    t.equal(buf.length, 4);
    t.same(v4, addr);
    // IPv6
    buf = types.inet.serialize(v6);
    t.equal(buf.length, 16);
    addr = types.inet.deserialize(buf);
    t.same(v6, addr);

    t.throws(function() {
      types.inet.serialize('foo');
    });
    t.end();
  });
});

