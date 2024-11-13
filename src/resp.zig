const std = @import("std");
const testing = std.testing;

pub const Value = union(enum) {
    simple: []const u8,
    string: []const u8,
    @"error": []const u8,
    array: []Value,
    null_string,

    pub fn deinit(self: Value, allocator: std.mem.Allocator) void {
        switch (self) {
            .simple, .string, .@"error" => |s| allocator.free(s),
            .array => |a| {
                for (a) |v| v.deinit(allocator);
                allocator.free(a);
            },
            .null_string => {},
        }
    }

    // pub fn initArray(allocator: std.mem.Allocator, n: usize) !Value {
    //     var array = try allocator.alloc(Value, n);
    //     for (0..array.len) |i| {
    //         array[i] = .null_string;
    //     }
    //     return .{ .array = array };
    // }
    //
    pub fn toOwned(self: *Value) Value {
        switch (self.*) {
            .simple => |s| {
                self.simple = "";
                return .{ .simple = s };
            },
            .string => |s| {
                self.string = "";
                return .{ .string = s };
            },
            .@"error" => |e| {
                self.@"error" = "";
                return .{ .@"error" = e };
            },
            .array => |a| {
                self.array = &.{};
                return .{ .array = a };
            },
            .null_string => {
                return .null_string;
            },
        }
    }

    pub fn write(self: Value, writer: std.io.AnyWriter) !void {
        switch (self) {
            .simple => |s| {
                try writer.print("+{s}\r\n", .{s});
            },
            .string => |s| {
                try writer.print("${d}\r\n{s}\r\n", .{ s.len, s });
            },
            .array => |a| {
                try writer.print("*{d}\r\n", .{a.len});
                for (a) |v| {
                    try v.write(writer);
                }
            },
            .null_string => {
                try writer.writeAll("$-1\r\n");
            },
            .@"error" => |e| {
                try writeErr(writer, "{s}", .{e});
            },
        }
    }

    pub fn writeErr(w: std.io.AnyWriter, comptime format: []const u8, args: anytype) !void {
        return w.print("-" ++ format ++ "\r\n", args);
    }

    pub fn read(allocator: std.mem.Allocator, reader: std.io.AnyReader) !Value {
        const first_byte = try reader.readByte();
        switch (first_byte) {
            '$' => {
                const len = try readLen(reader) orelse {
                    return .{ .string = "" };
                };
                const data = try allocator.alloc(u8, len);
                errdefer allocator.free(data);
                try reader.readNoEof(data);
                try readSeq(reader, "\r\n");
                return .{ .string = data };
            },
            '-' => {
                return .{ .@"error" = try readLine(allocator, reader) };
            },
            '+' => {
                return .{ .simple = try readLine(allocator, reader) };
            },
            '*' => {
                const len = try readLen(reader) orelse {
                    return error.InvalidChar;
                };
                var array = std.ArrayList(Value).init(allocator);
                defer {
                    for (array.items) |v| v.deinit(allocator);
                    array.deinit();
                }
                try array.ensureTotalCapacity(len);
                for (0..len) |_| {
                    const value = try read(allocator, reader);
                    errdefer value.deinit(allocator);
                    try array.append(value);
                }
                return .{ .array = try array.toOwnedSlice() };
            },
            else => return error.UnsuportedMessage,
        }
    }

    fn readLen(reader: std.io.AnyReader) !?usize {
        const first = try reader.readByte();
        if (first == '-') {
            try readSeq(reader, "1");
            return null;
        }
        if (!std.ascii.isDigit(first)) {
            return error.InvalidChar;
        }
        var len: usize = first - '0';
        while (true) {
            const b = try reader.readByte();
            if (!std.ascii.isDigit(b)) {
                if (b != '\r') return error.InvalidChar;
                break;
            }
            len *= 10;
            len += b - '0';
        }
        try readSeq(reader, "\n");
        return len;
    }

    fn readSeq(reader: std.io.AnyReader, comptime seq: []const u8) !void {
        inline for (seq) |b| {
            if (b != try reader.readByte()) return error.InvalidChar;
        }
    }

    fn readLine(allocator: std.mem.Allocator, reader: std.io.AnyReader) ![]u8 {
        var list = std.ArrayList(u8).init(allocator);
        defer list.deinit();
        var prev: u8 = 0;
        while (true) {
            const byte: u8 = try reader.readByte();
            if (byte == '\n') {
                break;
            }
            try list.append(byte);
            prev = byte;
        }
        if (prev == '\r') {
            _ = list.pop();
        }
        return list.toOwnedSlice();
    }
};

pub const Request = struct {
    raw: Value,
    allocator: std.mem.Allocator,
    name: []const u8,
    args: []Value,

    pub fn deinit(self: Request) void {
        self.raw.deinit(self.allocator);
    }

    pub fn read(allocator: std.mem.Allocator, reader: std.io.AnyReader) !Request {
        const raw = try Value.read(allocator, reader);
        errdefer raw.deinit(allocator);
        if (raw != .array or raw.array.len == 0 or raw.array[0] != .string) {
            return error.InvalidRequest;
        }
        return .{
            .allocator = allocator,
            .raw = raw,
            .name = raw.array[0].string,
            .args = raw.array[1..],
        };
    }
};

test "Value.readLine" {
    const input = "a\r\nb\nc\r\n";
    var fbs = std.io.fixedBufferStream(input);
    const r = fbs.reader();

    const a = try Value.readLine(testing.allocator, r.any());
    defer testing.allocator.free(a);
    try testing.expectEqualStrings("a", a);

    const b = try Value.readLine(testing.allocator, r.any());
    defer testing.allocator.free(b);
    try testing.expectEqualStrings("b", b);

    const c = try Value.readLine(testing.allocator, r.any());
    defer testing.allocator.free(c);
    try testing.expectEqualStrings("c", c);
}

test "Value.readLen" {
    var fbs = std.io.fixedBufferStream("123\r\n");
    const len = try Value.readLen(fbs.reader().any());
    try testing.expectEqual(len, 123);
}

test "Value.string: empty" {
    var fbs = std.io.fixedBufferStream("$0\r\n\r\n");

    const value = try Value.read(testing.allocator, fbs.reader().any());
    defer value.deinit(testing.allocator);

    try testing.expectEqualStrings("", value.string);
}

test "Value.string: data" {
    var fbs = std.io.fixedBufferStream("$5\r\nhello\r\n");

    const value = try Value.read(testing.allocator, fbs.reader().any());
    defer value.deinit(testing.allocator);

    try testing.expectEqualStrings("hello", value.string);
}

test "Value.string: null" {
    var fbs = std.io.fixedBufferStream("$-1\r\n");

    const value = try Value.read(testing.allocator, fbs.reader().any());
    defer value.deinit(testing.allocator);

    try testing.expectEqualStrings("", value.string);
}

test "Value.string: two" {
    var fbs = std.io.fixedBufferStream("$5\r\nhello\r\n$5\r\nworld\r\n");

    const value1 = try Value.read(testing.allocator, fbs.reader().any());
    defer value1.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", value1.string);

    const value2 = try Value.read(testing.allocator, fbs.reader().any());
    defer value2.deinit(testing.allocator);
    try testing.expectEqualStrings("world", value2.string);
}

test "Value.error" {
    var fbs = std.io.fixedBufferStream("-ERROR\r\n");
    const value = try Value.read(testing.allocator, fbs.reader().any());
    defer value.deinit(testing.allocator);
    try testing.expectEqualStrings("ERROR", value.@"error");
}

test "Value.array: empty" {
    var fbs = std.io.fixedBufferStream("*0\r\n");
    const value = try Value.read(testing.allocator, fbs.reader().any());
    defer value.deinit(testing.allocator);
    try testing.expectEqualDeep(value.array, @as([]Value, &.{}));
}

test "Value.array: strings" {
    var fbs = std.io.fixedBufferStream("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
    const value = try Value.read(testing.allocator, fbs.reader().any());
    defer value.deinit(testing.allocator);
    try testing.expectEqualDeep(value.array, @as([]const Value, &.{
        .{ .string = "hello" },
        .{ .string = "world" },
    }));
}

test "Request.read" {
    var fbs = std.io.fixedBufferStream("*2\r\n$4\r\nECHO\r\n$5\r\nworld\r\n");
    const req = try Request.read(testing.allocator, fbs.reader().any());
    defer req.deinit();
    try testing.expectEqualStrings(req.name, "ECHO");
    try testing.expectEqualDeep(req.args, @as([]const Value, &.{.{ .string = "world" }}));
}

test "Value.toOwned" {
    var value = Value{ .string = try testing.allocator.alloc(u8, 10) };
    defer value.deinit(testing.allocator);
    const owned = value.toOwned();
    defer owned.deinit(testing.allocator);
}
