const std = @import("std");
const resp = @import("resp.zig");
const stream = @import("stream.zig");

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

pub const XAdd = struct {
    key: resp.Value,
    id: stream.StreamID.Parsed,
    record: ?stream.Record,

    pub fn deinit(self: XAdd, allocator: std.mem.Allocator) void {
        self.key.deinit(allocator);
        if (self.record) |rec| {
            rec.deinit(allocator);
        }
    }

    pub fn toOwnedRecord(self: *XAdd) ?stream.Record {
        if (self.record) |rec| {
            self.record = null;
            return rec;
        }
        return null;
    }

    pub fn parse(args: []resp.Value, allocator: std.mem.Allocator) !XAdd {
        if (args.len < 2 or args.len % 2 != 0) {
            return error.InvalidArgs;
        }
        for (args) |a| {
            if (a != .string) {
                return error.InvalidArgs;
            }
        }
        const id = try stream.StreamID.parse(args[1].string);
        var rec = stream.Record.init(allocator);
        errdefer rec.deinit(allocator);

        for (args[2..]) |*value| {
            const owned = value.toOwned();
            errdefer owned.deinit(allocator);
            try rec.data.append(owned);
        }

        return .{
            .key = args[0].toOwned(),
            .id = id,
            .record = rec,
        };
    }
};

pub const XRange = struct {
    key: resp.Value,
    start: ?stream.StreamID,
    end: ?stream.StreamID,

    pub fn deinit(self: XRange, allocator: std.mem.Allocator) void {
        self.key.deinit(allocator);
    }

    pub fn parse(args: []resp.Value) !XRange {
        if (args.len != 3) {
            return error.InvalidArgs;
        }
        for (args) |arg| {
            if (arg != .string) {
                return error.InvalidArgs;
            }
        }
        const start = try parseID(args[1].string);
        const end = try parseID(args[2].string);
        return .{
            .key = args[0].toOwned(),
            .start = start,
            .end = end,
        };
    }

    fn parseID(input: []const u8) !?stream.StreamID {
        if (std.mem.eql(u8, input, "+") or std.mem.eql(u8, input, "-")) {
            return null;
        }
        const parsed = stream.StreamID.parse(input) catch {
            const timestamp = try std.fmt.parseInt(u64, input, 10);
            return .{
                .timestamp = timestamp,
                .sequence = 0,
            };
        };
        if (parsed.timestamp == null or parsed.sequence == null) {
            return error.InvalidArgs;
        }
        return .{
            .timestamp = parsed.timestamp.?,
            .sequence = parsed.sequence.?,
        };
    }
};
