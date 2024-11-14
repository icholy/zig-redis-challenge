const std = @import("std");
const testing = std.testing;

const EOF = 0xFF;
const AUX = 0xFA;
const RESIZEDB = 0xFB;
const EXPIRETIMEMS = 0xFC;
const EXPIRETIME = 0xFD;
const SELECTDB = 0xFE;

const Int = enum { i8, i16, i32 };

const Len = union(enum) {
    length: usize,
    int: Int,
    special: u8,
};

const KeyValuePair = struct {
    expires: u64 = 0,
    key: []const u8,
    value: []const u8,

    pub fn deinit(self: KeyValuePair, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
    }
};

// empty RDB file
pub const EMPTY = [_]u8{
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72,
    0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32,
    0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69,
    0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
    0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d,
    0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66,
    0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
    0xc0, 0xff, 0x5a, 0xa2,
};

// See: https://rdb.fnordig.de/file_format.html

pub const File = struct {
    version: i16,
    allocator: std.mem.Allocator,
    database: u8 = 0,
    aux: std.StringHashMap([]const u8),
    pairs: std.ArrayList(KeyValuePair),

    pub fn deinit(self: *File) void {
        var it = self.aux.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.*);
        }
        self.aux.deinit();
        for (self.pairs.items) |pair| {
            pair.deinit(self.allocator);
        }
        self.pairs.deinit();
    }

    pub fn read(allocator: std.mem.Allocator, r: std.io.AnyReader) !File {
        // read header
        var header: [9]u8 = undefined;
        _ = try r.readNoEof(&header);
        if (!std.mem.eql(u8, header[0..5], "REDIS")) {
            return error.BadHeader;
        }
        const version = try std.fmt.parseInt(i16, header[5..], 10);
        var file = File{
            .version = version,
            .allocator = allocator,
            .aux = std.StringHashMap([]const u8).init(allocator),
            .pairs = std.ArrayList(KeyValuePair).init(allocator),
        };
        errdefer file.deinit();
        loop: while (true) {
            const op = try r.readByte();
            switch (op) {
                EOF => {
                    var crc: [8]u8 = undefined;
                    _ = try r.readNoEof(&crc);
                    break :loop;
                },
                SELECTDB => {
                    file.database = try r.readByte();
                },
                EXPIRETIME => {
                    const expires = try readInt(u32, r, .little);
                    const vtype = try r.readByte();
                    var pair = try readKeyValue(allocator, r, vtype);
                    errdefer pair.deinit(allocator);
                    pair.expires = expires * 1000;
                    try file.pairs.append(pair);
                },
                EXPIRETIMEMS => {
                    const expires = try readInt(u64, r, .little);
                    const vtype = try r.readByte();
                    var pair = try readKeyValue(allocator, r, vtype);
                    errdefer pair.deinit(allocator);
                    pair.expires = expires;
                    try file.pairs.append(pair);
                },
                RESIZEDB => {
                    // TODO: pre-allocate the pair space using this info.
                    _ = try readVarInt(r);
                    _ = try readVarInt(r);
                },
                AUX => {
                    const key = try readString(allocator, r);
                    errdefer allocator.free(key);
                    const value = try readString(allocator, r);
                    errdefer allocator.free(value);
                    try file.aux.put(key, value);
                },
                else => {
                    const pair = try readKeyValue(allocator, r, op);
                    errdefer pair.deinit(allocator);
                    try file.pairs.append(pair);
                },
            }
        }
        return file;
    }

    fn readVarLen(r: std.io.AnyReader) !Len {
        const first: usize = @intCast(try r.readByte());
        return switch (first >> 6) {
            0b00 => return .{ .length = first },
            0b01 => {
                const second: usize = @intCast(try r.readByte());
                return .{ .length = ((first & 0b00111111) << 8) | second };
            },
            0b10 => return .{ .length = try readInt(u32, r, .big) },
            0b11 => {
                return switch (first & 0b00111111) {
                    0 => .{ .int = .i8 },
                    1 => .{ .int = .i16 },
                    2 => .{ .int = .i32 },
                    3 => .{ .special = @intCast(first & 0b00111111) },
                    else => error.InvalidByte,
                };
            },
            else => error.InvalidByte,
        };
    }

    fn readInt(comptime T: type, r: std.io.AnyReader, endian: std.builtin.Endian) !T {
        var buf: [@sizeOf(T)]u8 = undefined;
        _ = try r.readNoEof(&buf);
        return std.mem.readInt(T, &buf, endian);
    }

    fn readVarInt(r: std.io.AnyReader) !i32 {
        const len = try readVarLen(r);
        return switch (len) {
            .length => |v| @intCast(v),
            .int => |i| switch (i) {
                .i8 => @intCast(try readInt(i8, r, .big)),
                .i16 => @intCast(try readInt(i16, r, .big)),
                .i32 => @intCast(try readInt(i32, r, .big)),
            },
            .special => error.InvalidIntLen,
        };
    }

    fn readString(allocator: std.mem.Allocator, r: std.io.AnyReader) ![]const u8 {
        const len = try readVarLen(r);
        return switch (len) {
            .length => |n| {
                const str = try allocator.alloc(u8, n);
                errdefer allocator.free(str);
                try r.readNoEof(str);
                return str;
            },
            .int => |i| switch (i) {
                .i8 => {
                    return std.fmt.allocPrint(allocator, "{d}", .{
                        try readInt(i8, r, .little),
                    });
                },
                .i16 => {
                    return std.fmt.allocPrint(allocator, "{d}", .{
                        try readInt(i16, r, .little),
                    });
                },
                .i32 => {
                    return std.fmt.allocPrint(allocator, "{d}", .{
                        try readInt(i32, r, .little),
                    });
                },
            },
            .special => error.NotImplemented,
        };
    }

    fn readKeyValue(allocator: std.mem.Allocator, r: std.io.AnyReader, vtype: u8) !KeyValuePair {
        if (vtype != 0) {
            return error.NotImplemented;
        }
        const key = try readString(allocator, r);
        errdefer allocator.free(key);
        const value = try readString(allocator, r);
        return .{ .key = key, .value = value };
    }
};

test "File.read: Bad magic string" {
    var fbs = std.io.fixedBufferStream("bad magic string");
    const err = File.read(testing.failing_allocator, fbs.reader().any());
    try testing.expectError(error.BadHeader, err);
}

test "File.readVarLen: 0b00" {
    var fbs = std.io.fixedBufferStream(&[_]u8{0b00000011});
    const len = try File.readVarLen(fbs.reader().any());
    try testing.expectEqual(Len{ .length = 3 }, len);
}

test "File.readVarLen: 0b01" {
    var fbs = std.io.fixedBufferStream(&[_]u8{ 0b01000001, 0b00000001 });
    const len = try File.readVarLen(fbs.reader().any());
    try testing.expectEqual(Len{ .length = 257 }, len);
}

test "File.readVarLen: 0b10" {
    var fbs = std.io.fixedBufferStream(&[_]u8{ 0b10000000, 0b00000000, 0b00000001, 0b00000000, 0b00000001 });
    const len = try File.readVarLen(fbs.reader().any());
    try testing.expectEqual(Len{ .length = 65537 }, len);
}

test "File.readVarLen: i16" {
    var fbs = std.io.fixedBufferStream(&[_]u8{0b11000001});
    const len = try File.readVarLen(fbs.reader().any());
    try testing.expectEqualDeep(Len{ .int = .i16 }, len);
}

test "File.readString: length prefixed" {
    var fbs = std.io.fixedBufferStream(.{5} ++ "hello");
    const str = try File.readString(testing.allocator, fbs.reader().any());
    defer testing.allocator.free(str);
    try testing.expectEqualStrings("hello", str);
}

test "File.readKeyValue" {
    var fbs = std.io.fixedBufferStream(.{5} ++ "hello" ++ .{5} ++ "world");
    const pair = try File.readKeyValue(testing.allocator, fbs.reader().any(), 0);
    defer pair.deinit(testing.allocator);
    try testing.expectEqualDeep(KeyValuePair{ .key = "hello", .value = "world" }, pair);
}

test "File.read: database selector" {
    var fbs = std.io.fixedBufferStream("REDIS0003" ++ .{ SELECTDB, 1, EOF } ++ .{0} ** 8);
    var file = try File.read(testing.allocator, fbs.reader().any());
    defer file.deinit();
    try testing.expectEqual(1, file.database);
}

test "File.read: aux" {
    var fbs = std.io.fixedBufferStream("REDIS0003" ++ .{ AUX, 1, 'a', 2, 'b', 'b', EOF } ++ .{0} ** 8);
    var file = try File.read(testing.allocator, fbs.reader().any());
    defer file.deinit();
    try testing.expectEqualStrings("bb", file.aux.get("a").?);
}

test "File.read: pair" {
    var fbs = std.io.fixedBufferStream("REDIS0003" ++ .{ EXPIRETIME, 0, 0, 0, 5 } ++ .{ 0, 1, 'a', 2, 'b', 'b', EOF } ++ .{0} ** 8);
    var file = try File.read(testing.allocator, fbs.reader().any());
    defer file.deinit();
    try testing.expectEqual(file.pairs.items.len, 1);
    const pair = file.pairs.items[0];
    try testing.expectEqualDeep(KeyValuePair{ .expires = 5000, .key = "a", .value = "bb" }, pair);
}

test "File.read: real_1" {
    var fbs = std.io.fixedBufferStream(&[_]u8{
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x39,
        0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76,
        0x65, 0x72, 0x06, 0x36, 0x2e, 0x30, 0x2e, 0x31, 0x36,
        0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62,
        0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74,
        0x69, 0x6d, 0x65, 0xc2, 0x57, 0xd8, 0x30, 0x67, 0xfa,
        0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d,
        0xc2, 0x98, 0xad, 0x0c, 0x00, 0xfa, 0x0c, 0x61, 0x6f,
        0x66, 0x2d, 0x70, 0x72, 0x65, 0x61, 0x6d, 0x62, 0x6c,
        0x65, 0xc0, 0x00, 0xfe, 0x00, 0xfb, 0x01, 0x01, 0xfc,
        0x00, 0x74, 0xce, 0x16, 0x93, 0x01, 0x00, 0x00, 0x00,
        0x03, 0x66, 0x6f, 0x6f, 0x03, 0x62, 0x61, 0x72, 0xff,
        0x8d, 0xcd, 0x0c, 0xab, 0xd9, 0x90, 0xf5, 0xef,
    });
    var file = try File.read(testing.allocator, fbs.reader().any());
    defer file.deinit();
    try testing.expectEqual(1, file.pairs.items.len);
    try testing.expectEqualDeep(KeyValuePair{
        .key = "foo",
        .value = "bar",
        .expires = 32877693649354752,
    }, file.pairs.items[0]);
}

test "File.read: real_2" {
    var fbs = std.io.fixedBufferStream(&[_]u8{
        // header
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x39,

        // AUX
        0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76,
        0x65, 0x72, 0x06, 0x36, 0x2e, 0x30, 0x2e, 0x31, 0x36,

        // AUX
        0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62,
        0x69, 0x74, 0x73, 0xc0, 0x40,

        // AUX
        0xfa, 0x05, 0x63, 0x74,
        0x69, 0x6d, 0x65, 0xc2, 0xab, 0xfd, 0x30, 0x67,

        // AUX
        0xfa,
        0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d,
        0xc2, 0xb8, 0x5a, 0x0c, 0x00,

        // AUX
        0xfa, 0x0c, 0x61, 0x6f,
        0x66, 0x2d, 0x70, 0x72, 0x65, 0x61, 0x6d, 0x62, 0x6c,
        0x65, 0xc0, 0x00,

        // SELECTDB
        0xfe, 0x00,

        // RESIZEDB
        0xfb, 0x01, 0x00,

        // PAIR
        0x00,
        0x05, 0x6d, 0x79, 0x6b, 0x65, 0x79, 0x05, 0x6d, 0x79,
        0x76, 0x61, 0x6c,

        // ff
        0xff, 0xae, 0xc4, 0x1e, 0x63, 0x93,
        0x79, 0xd9, 0x72,
    });
    var file = try File.read(testing.allocator, fbs.reader().any());
    defer file.deinit();
    try testing.expectEqual(1, file.pairs.items.len);
    try testing.expectEqualDeep(KeyValuePair{
        .key = "mykey",
        .value = "myval",
        .expires = 0,
    }, file.pairs.items[0]);
}
