const std = @import("std");
const testing = std.testing;
const RadixTree = @import("radixtree.zig").RadixTree;

pub const StreamID = struct {
    timestamp: u64,
    sequence: u64,

    fn decode(encoded: [16]u8) StreamID {
        return .{
            .timestamp = std.mem.readInt(u64, encoded[0..8], .big),
            .sequence = std.mem.readInt(u64, encoded[8..], .big),
        };
    }

    fn encode(self: StreamID) [16]u8 {
        var encoded: [16]u8 = undefined;
        std.mem.writeInt(u64, encoded[0..8], self.timestamp, .big);
        std.mem.writeInt(u64, encoded[8..], self.sequence, .big);
        return encoded;
    }

    fn parse(input: []const u8) !StreamID {
        const index = std.mem.indexOf(u8, input, "-") orelse {
            // no sequence number
            return .{
                .timestamp = try std.fmt.parseUnsigned(u64, input, 10),
                .sequence = 0,
            };
        };
        return .{
            .timestamp = try std.fmt.parseUnsigned(u64, input[0..index], 10),
            .sequence = try std.fmt.parseUnsigned(u64, input[index + 1 ..], 10),
        };
    }
};

pub const Record = struct {
    pub const Pair = struct {
        key: []const u8,
        value: []const u8,
    };

    pairs: std.ArrayList(Pair),

    pub fn init(allocator: std.mem.Allocator) Record {
        return .{
            .pairs = std.ArrayList(Pair).init(allocator),
        };
    }

    pub fn deinit(self: *Record) void {
        self.pairs.deinit();
    }
};

pub const Stream = struct {
    records: RadixTree(*Record),

    pub fn init(allocator: std.mem.Allocator) Stream {
        return .{
            .records = RadixTree(*Record).init(allocator),
        };
    }

    pub fn deinit(self: *Stream) void {
        self.records.deinit();
    }

    pub fn insert(self: *Stream, id: StreamID, rec: *Record) !void {
        const seq = id.encode();
        try self.records.insert(&seq, rec);
    }
};

test "StreamID.encode: boundary" {
    try testing.expectEqual(
        [1]u8{0} ** 16,
        StreamID.encode(.{ .timestamp = 0, .sequence = 0 }),
    );
    try testing.expectEqual(
        [1]u8{0xFF} ** 16,
        StreamID.encode(.{ .timestamp = std.math.maxInt(u64), .sequence = std.math.maxInt(u64) }),
    );
}

test "StreamID.encode: order" {
    const Test = struct {
        lo: StreamID,
        hi: StreamID,
    };
    const tests = [_]Test{
        .{
            .lo = .{ .timestamp = 0, .sequence = 0 },
            .hi = .{ .timestamp = 1, .sequence = 1 },
        },
        .{
            .lo = .{ .timestamp = 0, .sequence = 1 },
            .hi = .{ .timestamp = 1, .sequence = 0 },
        },
        .{
            .lo = .{ .timestamp = 1, .sequence = 0 },
            .hi = .{ .timestamp = 1, .sequence = 1 },
        },
        .{
            .lo = .{ .timestamp = std.math.maxInt(u64) - 1, .sequence = std.math.maxInt(u64) },
            .hi = .{ .timestamp = std.math.maxInt(u64), .sequence = 0 },
        },
    };
    for (tests) |t| {
        const lo = t.lo.encode();
        const hi = t.hi.encode();
        try testing.expectEqual(std.mem.order(u8, &lo, &hi), .lt);
    }
}

test "StreamID.parse: 1" {
    try testing.expectEqual(
        StreamID{ .timestamp = 1, .sequence = 2 },
        try StreamID.parse("1-2"),
    );
    try testing.expectEqual(
        StreamID{ .timestamp = 1, .sequence = 0 },
        try StreamID.parse("1"),
    );
}

test "Stream.insert" {
    var stream = Stream.init(testing.allocator);
    defer stream.deinit();
    var rec = Record.init(testing.allocator);
    defer rec.deinit();
    try stream.insert(.{ .timestamp = 1, .sequence = 0 }, &rec);
}
