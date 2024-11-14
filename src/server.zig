const std = @import("std");
const resp = @import("resp.zig");
const util = @import("util.zig");
const command = @import("command.zig");
const Stream = @import("stream.zig").Stream;
const StreamID = @import("stream.zig").StreamID;
const StreamRecord = @import("stream.zig").Record;
const testing = std.testing;

pub const Server = struct {
    const ValueData = union(enum) {
        value: resp.Value,
        stream: *Stream,

        pub fn deinit(self: ValueData, allocator: std.mem.Allocator) void {
            switch (self) {
                .value => |v| v.deinit(allocator),
                .stream => |s| s.deinit(),
            }
        }
    };

    const Value = struct {
        data: ValueData,
        expires: u64,
    };

    pub const Config = struct {
        dir: []const u8 = "",
        dbfilename: []const u8 = "",
    };

    allocator: std.mem.Allocator,
    values: std.StringHashMap(Value),
    config: Config,
    mutex: std.Thread.Mutex = .{},

    pub fn init(allocator: std.mem.Allocator, config: Config) Server {
        return .{
            .allocator = allocator,
            .values = std.StringHashMap(Value).init(allocator),
            .config = config,
        };
    }

    pub fn deinit(self: *Server) void {
        var it = self.values.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.data.deinit(self.allocator);
        }
        self.values.deinit();
    }

    pub fn serve(self: *Server, conn: std.net.Server.Connection) void {
        defer conn.stream.close();
        const r = conn.stream.reader().any();
        const w = conn.stream.writer().any();
        while (true) {
            self.next(r, w) catch |err| switch (err) {
                error.EndOfStream => return,
                else => {
                    std.debug.print("next error: {s}\n", .{@errorName(err)});
                    return;
                },
            };
        }
    }

    pub fn next(self: *Server, r: std.io.AnyReader, w: std.io.AnyWriter) !void {
        const req = try resp.Request.read(self.allocator, r);
        defer req.deinit();
        std.debug.print("request: {s}, args: {any}\n", .{ req.name, req.args });
        self.handle(req, w) catch |err| {
            try resp.Value.writeErr(w, "ERR: failed to process request {s}", .{@errorName(err)});
        };
    }

    fn handle(self: *Server, req: resp.Request, w: std.io.AnyWriter) !void {
        if (req.is("COMMAND")) {
            return self.onCommand(w);
        }
        if (req.is("PING")) {
            return self.onPing(w);
        }
        if (req.is("ECHO")) {
            return self.onEcho(w, req.args);
        }
        if (req.is("SET")) {
            return self.onSet(w, req.args);
        }
        if (req.is("GET")) {
            return self.onGet(w, req.args);
        }
        if (req.is("DUMP")) {
            return self.onDump(w);
        }
        if (req.is("CONFIG")) {
            return self.onConfig(w, req.args);
        }
        if (req.is("KEYS")) {
            return self.onKeys(w, req.args);
        }
        if (req.is("TYPE")) {
            return self.onType(w, req.args);
        }
        if (req.is("XADD")) {
            return self.onXAdd(w, req.args);
        }
        if (req.is("XRANGE")) {
            return self.onXRange(w, req.args);
        }
        if (req.is("XREAD")) {
            return self.onXRead(w, req.args);
        }
        try resp.Value.writeErr(w, "ERR: unrecognised command: {s}", .{req.name});
    }

    fn onCommand(_: *Server, w: std.io.AnyWriter) !void {
        try resp.Value.write(.null_string, w);
    }

    fn onPing(_: *Server, w: std.io.AnyWriter) !void {
        try resp.Value.write(.{ .simple = "PONG" }, w);
    }

    fn onEcho(_: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        if (args.len != 1) {
            return error.InvalidEcho;
        }
        try args[0].write(w);
    }

    fn onSet(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        const cmd = try command.Set.parse(args);
        errdefer cmd.deinit(self.allocator);
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            var expires: u64 = 0;
            if (cmd.ttl > 0) {
                expires = @as(u64, @intCast(std.time.milliTimestamp())) + cmd.ttl;
            }
            const prev = try self.values.fetchPut(cmd.key, .{
                .expires = expires,
                .data = .{ .value = cmd.value },
            });
            if (prev) |kv| {
                self.allocator.free(kv.key);
                kv.value.data.deinit(self.allocator);
            }
        }
        try resp.Value.write(.{ .simple = "OK" }, w);
    }

    fn onGet(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        if (args.len != 1 or args[0] != .string) {
            return error.InvalidGet;
        }
        self.mutex.lock();
        defer self.mutex.unlock();
        const key = args[0].string;
        const entry = self.values.getEntry(key) orelse {
            try resp.Value.write(.null_string, w);
            return;
        };
        const value: Value = entry.value_ptr.*;
        if (value.expires > 0 and std.time.milliTimestamp() > value.expires) {
            self.values.removeByPtr(entry.key_ptr);
            try resp.Value.write(.null_string, w);
            return;
        }
        switch (value.data) {
            .value => |v| {
                if (v != .string) {
                    try resp.Value.writeErr(w, "only strings are supported", .{});
                    return;
                }
                try v.write(w);
            },
            .stream => {
                try resp.Value.writeErr(w, "cannot GET from stream", .{});
            },
        }
    }

    fn onDump(self: *Server, w: std.io.AnyWriter) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        var it = self.values.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            switch (entry.value_ptr.data) {
                .value => |value| std.debug.print("VALUE {s} = {any}\n", .{ key, value }),
                .stream => |stream| {
                    std.debug.print("STREAM {s}\n", .{key});
                    var it2 = try stream.tree.iterator();
                    defer it2.deinit();
                    while (try it2.next()) |entry2| {
                        const id = StreamID.decode(entry2.seq[0..16].*);
                        std.debug.print("ENTRY: id='{d}-{d}' =>", .{ id.timestamp, id.sequence });
                        for (entry2.value.data.items) |value| {
                            std.debug.print(" {any}", .{value.string});
                        }
                        std.debug.print("\n", .{});
                    }
                },
            }
        }
        try resp.Value.write(.{ .simple = "OK" }, w);
    }

    fn onConfig(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        if (args.len != 2 or args[0] != .string or args[1] != .string) {
            return error.InvalidArgs;
        }
        if (util.ieql(args[0].string, "GET")) {
            return self.onConfigGet(w, args[1].string);
        }
        try resp.Value.writeErr(w, "Not implemented", .{});
    }

    fn onConfigGet(self: *Server, w: std.io.AnyWriter, name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (std.mem.eql(u8, name, "dir")) {
            var config = [_]resp.Value{
                .{ .string = name },
                .{ .string = self.config.dir },
            };
            try resp.Value.write(.{ .array = &config }, w);
            return;
        }

        if (std.mem.eql(u8, name, "dbfilename")) {
            var config = [_]resp.Value{
                .{ .string = name },
                .{ .string = self.config.dbfilename },
            };
            try resp.Value.write(.{ .array = &config }, w);
            return;
        }

        try resp.Value.writeErr(w, "Not implemented: {s}", .{name});
    }

    fn onKeys(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        if (args.len != 1 or args[0] != .string) {
            return error.InvalidArgs;
        }
        const pattern = args[0].string;
        if (!std.mem.eql(u8, pattern, "*")) {
            try resp.Value.writeErr(w, "only '*' pattern is supported", .{});
        }
        self.mutex.lock();
        defer self.mutex.unlock();
        var keys = std.ArrayList(resp.Value).init(self.allocator);
        defer keys.deinit();
        try keys.ensureTotalCapacity(self.values.count());
        var it = self.values.keyIterator();
        while (it.next()) |key| {
            try keys.append(.{ .string = key.* });
        }
        try resp.Value.write(.{ .array = keys.items }, w);
    }

    fn onType(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        if (args.len != 1 or args[0] != .string) {
            return error.InvalidArgs;
        }
        const key = args[0].string;
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.values.get(key)) |v| {
            switch (v.data) {
                .value => {
                    try resp.Value.write(.{ .simple = "string" }, w);
                },
                .stream => {
                    try resp.Value.write(.{ .simple = "stream" }, w);
                },
            }
        } else {
            try resp.Value.write(.{ .simple = "none" }, w);
        }
    }

    fn onXAdd(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        // parse the command
        var cmd = try command.XAdd.parse(args, self.allocator);
        defer cmd.deinit(self.allocator);
        // create the stream if it doesn't exist
        const stream = try self.getStream(cmd.key.string, true);
        stream.mutex.lock();
        defer stream.mutex.unlock();
        // parse the id
        var id = .{
            .timestamp = cmd.id.timestamp orelse 0,
            .sequence = cmd.id.sequence orelse 0,
        };
        if (cmd.id.timestamp == null) {
            id.timestamp = @intCast(std.time.milliTimestamp());
        }
        if (cmd.id.sequence == null and id.timestamp == stream.last.timestamp) {
            id.sequence = stream.last.sequence + 1;
        }
        if (id.timestamp == 0 and id.sequence == 0) {
            try resp.Value.writeErr(w, "ERR The ID specified in XADD must be greater than 0-0", .{});
            return;
        }
        if (stream.last.order(id) != .lt) {
            try resp.Value.writeErr(w, "ERR The ID specified in XADD is equal or smaller than the target stream top item", .{});
            return;
        }
        // insert the entry
        const rec = cmd.toOwnedRecord() orelse StreamRecord.init(self.allocator);
        errdefer rec.deinit(self.allocator);
        try stream.insert(id, rec);
        // return the generated id
        const formatted_id = try std.fmt.allocPrint(self.allocator, "{d}-{d}", .{ id.timestamp, id.sequence });
        defer self.allocator.free(formatted_id);
        try resp.Value.write(.{ .string = formatted_id }, w);
    }

    fn onXRange(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        // parse the request
        const cmd = try command.XRange.parse(args);
        defer cmd.deinit(self.allocator);
        // get the steam
        const stream = try self.getStream(cmd.key.string, false);
        stream.mutex.lock();
        defer stream.mutex.unlock();
        const output = try self.streamRead(stream, cmd.start, cmd.end);
        defer output.deinit(self.allocator);
        return output.write(w);
    }

    fn onXRead(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        var cmd = try command.XRead.parse(args, self.allocator);
        defer cmd.deinit(self.allocator);
        var outputs = std.ArrayList(resp.Value).init(self.allocator);
        defer {
            for (outputs.items) |v| {
                v.deinit(self.allocator);
            }
            outputs.deinit();
        }
        var streams = std.ArrayList(*Stream).init(self.allocator);
        defer {
            for (streams.items) |stream| {
                stream.mutex.unlock();
            }
            streams.deinit();
        }
        for (cmd.ops) |*op| {
            op.start.sequence += 1;

            const stream = try self.getStream(op.key.string, false);
            try streams.append(stream);
            stream.mutex.lock();

            // WARN: this implementation has two big flaws:
            //  1. This can't detect when a client disconnects.
            //  2. If the XREAD has multuple STREAMS, they all need to have data.
            if (cmd.block) |block| {
                stream.block(op.start, block * std.time.ns_per_ms) catch |err| {
                    if (err != error.Timeout) {
                        return err;
                    }
                    try resp.Value.write(.null_string, w);
                    return;
                };
            }

            const output = try resp.Value.initArray(self.allocator, 2);
            errdefer output.deinit(self.allocator);
            output.array[0] = .{ .borrowed_string = op.key.string };
            output.array[1] = try self.streamRead(stream, op.start, null);
            try outputs.append(output);
        }
        try resp.Value.write(.{ .array = outputs.items }, w);
    }

    fn getStream(self: *Server, key: []const u8, create: bool) !*Stream {
        self.mutex.lock();
        defer self.mutex.unlock();
        // create the stream if it doesn't exist
        if (self.values.get(key)) |value| {
            if (value.data != .stream) {
                return error.StreamNotFound;
            }
            return value.data.stream;
        }
        if (!create) {
            return error.StreamNotFound;
        }
        var stream = try self.allocator.create(Stream);
        errdefer self.allocator.destroy(stream);
        stream.* = Stream.init(self.allocator);
        errdefer stream.deinit();
        const key_dup = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_dup);
        try self.values.put(key_dup, .{
            .data = .{ .stream = stream },
            .expires = 0,
        });
        return stream;
    }

    fn streamRead(self: *Server, stream: *Stream, start: ?StreamID, end: ?StreamID) !resp.Value {
        // seek to the start id
        var it = try stream.tree.iterator();
        defer it.deinit();
        if (start) |id| {
            const encoded = id.encode();
            try it.seek(&encoded);
        }
        // iterate throug the values and build up the response array
        var entries = std.ArrayList(resp.Value).init(self.allocator);
        defer entries.deinit();
        errdefer {
            for (entries.items) |v| {
                v.deinit(self.allocator);
            }
        }
        while (try it.next()) |entry| {
            const id = StreamID.decode(entry.seq[0..16].*);
            if (end != null and id.order(end.?) == .gt) {
                break;
            }
            var resp_entry = try resp.Value.initArray(self.allocator, 2);
            errdefer resp_entry.deinit(self.allocator);
            resp_entry.array[0] = .{ .string = try id.format(self.allocator) };
            resp_entry.array[1] = .{ .borrowed_array = entry.value.data.items };
            try entries.append(resp_entry);
        }
        return .{ .array = try entries.toOwnedSlice() };
    }
};

test "Server PING" {
    var server = Server.init(testing.allocator, .{});
    defer server.deinit();
    var input = std.io.fixedBufferStream("*1\r\n$4\r\nPING\r\n");
    var output = std.ArrayList(u8).init(testing.allocator);
    defer output.deinit();
    try server.next(input.reader().any(), output.writer().any());
    var output_fbs = std.io.fixedBufferStream(output.items);
    const res = try resp.Value.read(testing.allocator, output_fbs.reader().any());
    defer res.deinit(testing.allocator);
    try testing.expectEqualDeep(res, resp.Value{ .simple = "PONG" });
}
