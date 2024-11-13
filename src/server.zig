const std = @import("std");
const resp = @import("resp.zig");
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
        try self.handle(req, w);
    }

    fn handle(self: *Server, req: resp.Request, w: std.io.AnyWriter) !void {
        if (std.mem.eql(u8, req.name, "COMMAND")) {
            return self.onCommand(w);
        }
        if (std.mem.eql(u8, req.name, "PING")) {
            return self.onPing(w);
        }
        if (std.mem.eql(u8, req.name, "ECHO")) {
            return self.onEcho(w, req.args);
        }
        if (std.mem.eql(u8, req.name, "SET")) {
            return self.onSet(w, req.args);
        }
        if (std.mem.eql(u8, req.name, "GET")) {
            return self.onGet(w, req.args);
        }
        if (std.mem.eql(u8, req.name, "DUMP")) {
            return self.onDump(w);
        }
        if (std.mem.eql(u8, req.name, "CONFIG")) {
            return self.onConfig(w, req.args);
        }
        if (std.mem.eql(u8, req.name, "KEYS")) {
            return self.onKeys(w, req.args);
        }
        if (std.mem.eql(u8, req.name, "TYPE")) {
            return self.onType(w, req.args);
        }
        if (std.mem.eql(u8, req.name, "XADD")) {
            return self.onXAdd(w, req.args);
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
                    std.debug.print("STREAM {s}", .{key});
                    _ = stream;
                    // const stdout = std.io.getStdOut();
                    // try stream.records.write(stdout.writer().any());
                },
            }
        }
        try resp.Value.write(.{ .simple = "OK" }, w);
    }

    fn onConfig(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        if (args.len != 2 or args[0] != .string or args[1] != .string) {
            return error.InvalidArgs;
        }
        if (std.mem.eql(u8, args[0].string, "GET")) {
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
        var cmd = try command.XAdd.parse(args, self.allocator);
        defer cmd.deinit(self.allocator);
        self.mutex.lock();
        defer self.mutex.unlock();
        var stream: *Stream = undefined;
        if (self.values.get(cmd.key.string)) |value| {
            if (value.data != .stream) {
                try resp.Value.writeErr(w, "'{s}' does not contain a stream", .{cmd.key.string});
                return;
            }
            stream = value.data.stream;
        } else {
            stream = try self.allocator.create(Stream);
            errdefer self.allocator.destroy(stream);
            stream.* = Stream.init(self.allocator);
            errdefer stream.deinit();
            const key = cmd.key.toOwned();
            errdefer key.deinit(self.allocator);
            try self.values.put(key.string, .{
                .data = .{ .stream = stream },
                .expires = 0,
            });
        }
        const rec = cmd.toOwnedRecord() orelse StreamRecord.init(self.allocator);
        errdefer rec.deinit(self.allocator);
        try stream.insert(cmd.id, rec);
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
