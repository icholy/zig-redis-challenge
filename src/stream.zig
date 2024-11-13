const std = @import("std");
const testing = std.testing;
const RadixTree = @import("radixtree.zig").RadixTree;

const StreamID = struct {
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
};

test "StreamID.encode: zero" {
    try testing.expectEqual([1]u8{0} ** 16, StreamID.encode(.{ .timestamp = 0, .sequence = 0 }));
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
