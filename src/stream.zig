const std = @import("std");
const testing = std.testing;
const RadixTree = @import("radixtree.zig").RadixTree;

const StreamID = struct {
    timestamp: u64,
    sequence: u64,

    fn encode(self: StreamID) [16]u8 {
        var encoded: [16]u8 = undefined;
        std.mem.writeInt(u64, encoded[0..8], self.timestamp, .big);
        std.mem.writeInt(u64, encoded[8..], self.timestamp, .big);
        return encoded;
    }
};

test "StreamID.encode" {
    try testing.expectEqual([1]u8{0} ** 16, StreamID.encode(.{ .timestamp = 0, .sequence = 0 }));
}
