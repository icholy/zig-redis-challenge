const std = @import("std");
const testing = std.testing;
const resp = @import("resp.zig");
const RadixTree = @import("radixtree.zig").RadixTree;

pub const StreamID = struct {
    pub const Parsed = struct {
        timestamp: ?u64,
        sequence: ?u64,
    };

    timestamp: u64,
    sequence: u64,

    pub fn decode(encoded: [16]u8) StreamID {
        return .{
            .timestamp = std.mem.readInt(u64, encoded[0..8], .big),
            .sequence = std.mem.readInt(u64, encoded[8..], .big),
        };
    }

    pub fn encode(self: StreamID) [16]u8 {
        var encoded: [16]u8 = undefined;
        std.mem.writeInt(u64, encoded[0..8], self.timestamp, .big);
        std.mem.writeInt(u64, encoded[8..], self.sequence, .big);
        return encoded;
    }

    pub fn order(self: StreamID, other: StreamID) std.math.Order {
        const ord = std.math.order(self.timestamp, other.timestamp);
        if (ord != .eq) {
            return ord;
        }
        return std.math.order(self.sequence, other.sequence);
    }

    pub fn format(self: StreamID, allocator: std.mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(allocator, "{d}-{d}", .{ self.timestamp, self.sequence });
    }

    pub fn parse(input: []const u8) !Parsed {
        if (std.mem.eql(u8, input, "*")) {
            return .{ .timestamp = null, .sequence = null };
        }
        if (std.mem.endsWith(u8, input, "-*")) {
            return .{
                .timestamp = try std.fmt.parseUnsigned(u64, input[0 .. input.len - 2], 10),
                .sequence = null,
            };
        }
        const index = std.mem.indexOf(u8, input, "-") orelse return error.InvalidStreamID;
        return .{
            .timestamp = try std.fmt.parseUnsigned(u64, input[0..index], 10),
            .sequence = try std.fmt.parseUnsigned(u64, input[index + 1 ..], 10),
        };
    }
};

pub const Record = struct {
    data: std.ArrayList(resp.Value),

    pub fn init(allocator: std.mem.Allocator) Record {
        return .{
            .data = std.ArrayList(resp.Value).init(allocator),
        };
    }

    pub fn deinit(self: Record, allocator: std.mem.Allocator) void {
        for (self.data.items) |value| {
            value.deinit(allocator);
        }
        self.data.deinit();
    }
};

pub const Stream = struct {
    last: StreamID,
    tree: RadixTree(Record),
    mutex: std.Thread.Mutex,
    condition: std.Thread.Condition,

    pub fn init(allocator: std.mem.Allocator) Stream {
        return .{
            .tree = RadixTree(Record).init(allocator),
            .last = .{ .timestamp = 0, .sequence = 0 },
            .mutex = .{},
            .condition = .{},
        };
    }

    pub fn deinit(self: *Stream) void {
        self.tree.deinit();
    }

    pub fn insert(self: *Stream, id: StreamID, rec: Record) !void {
        self.last = id;
        const seq = id.encode();
        try self.tree.insert(&seq, rec);
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
        StreamID.Parsed{ .timestamp = 1, .sequence = 2 },
        try StreamID.parse("1-2"),
    );
    try testing.expectEqual(
        StreamID.Parsed{ .timestamp = 1, .sequence = 0 },
        try StreamID.parse("1-0"),
    );
}

test "Stream.insert" {
    var stream = Stream.init(testing.allocator);
    defer stream.deinit();
    var rec = Record.init(testing.allocator);
    defer rec.deinit(testing.allocator);
    try stream.insert(.{ .timestamp = 1, .sequence = 0 }, rec);
}
