var test = require('tap').test,
    Buf = require('../lib/buffer').Buf;

test('Buf should expand automatically beyond initial capacity', function(t) {
  var buf = new Buf(1);
  buf.writeString('abc');
  buf.rewind();
  t.assert('abc', buf.readString());
  t.end();
});

test('test read/write byte', function(t) {
  var b = 1,
      buf = new Buf();

  buf.writeByte(b);
  buf.rewind();
  t.equals(b, buf.readByte());

  buf.rewind();
  t.throws(function() {
    buf.writeByte(null);
  }, 'cannot write other than number');
  t.end();
});

test('test read/write short', function(t) {
  var n = 2,
      buf = new Buf();

  buf.writeShort(n);
  buf.rewind();
  t.equals(n, buf.readShort());

  t.end();
});

test('test read/write int', function(t) {
  var n = 2,
      buf = new Buf();

  buf.writeInt(n);
  buf.rewind();
  t.equals(n, buf.readInt());

  t.end();
});

test('test read/write string', function(t) {
  var s = 'abcdefg',
      buf = new Buf();

  buf.writeString(s);
  buf.rewind();
  t.equals(s, buf.readString());

  t.end();
});

test('test read/write long string', function(t) {
  var s = 'abcdefg',
      buf = new Buf();

  buf.writeLongString(s);
  buf.rewind();
  t.equals(s, buf.readLongString());

  t.end();
});

test('test read/write string list', function(t) {
  var s = ['abc', 'defg'],
      buf = new Buf();

  buf.writeStringList(s);
  buf.rewind();
  t.same(s, buf.readStringList());

  t.end();
});

test('test read/write string map', function(t) {
  var s = {abc: 'defg'},
      buf = new Buf();

  buf.writeStringMap(s);
  buf.rewind();
  t.same(s, buf.readStringMap());

  t.end();
});

test('test read/write inet', function(t) {
  var v4 = {
              address: '10.10.10.10',
              port: 9140
            },
      v6 = {
              address: '2001:db8:1234::1',
              port: 9140
            },
      noport = {
              address: '10.10.10.10'
            },
      buf = new Buf();

  buf.writeInet(v4);
  buf.rewind();
  t.same(v4, buf.readInet());

  buf.rewind();

  buf.writeInet(v6);
  buf.rewind();
  t.same(v6, buf.readInet());

  buf.rewind();

  buf.writeInet(noport);
  buf.rewind();
  t.same(noport, buf.readInet());

  t.end();
});

test('test read/write uuid', function(t) {
  var uuid = '1255cf50-045d-11e3-8ffd-0800200c9a66',
      buf = new Buf();

  buf.writeUUID(uuid);
  buf.rewind();
  t.equals(uuid, buf.readUUID());

  t.end();
});
