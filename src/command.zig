const std = @import("std");
const resp = @import("resp.zig");

pub const Set = struct {
    key: []const u8,
    value: resp.Value,
    ttl: u64,

    pub fn deinit(self: Set, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        self.value.deinit(allocator);
    }

    pub fn parse(args: []resp.Value) !Set {
        if ((args.len != 2 and args.len != 4) or args[0] != .string) {
            return error.InvalidArgs;
        }
        var ttl: u64 = 0;
        if (args.len == 4) {
            ttl = try parse_ttl(args[2], args[3]);
        }
        return .{
            .key = args[0].toOwned().string,
            .value = args[1].toOwned(),
            .ttl = ttl,
        };
    }

    fn parse_ttl(unit: resp.Value, ttl: resp.Value) !u64 {
        if (unit != .string or ttl != .string) {
            return error.InvalidArgs;
        }
        if (std.mem.eql(u8, unit.string, "EX") or std.mem.eql(u8, unit.string, "ex")) {
            return (try std.fmt.parseInt(u64, ttl.string, 10)) * 1000;
        }
        if (std.mem.eql(u8, unit.string, "PX") or std.mem.eql(u8, unit.string, "px")) {
            return (try std.fmt.parseInt(u64, ttl.string, 10));
        }
        return error.InvalidArgs;
    }
};
