const std = @import("std");
const resp = @import("resp.zig");
const util = @import("util.zig");
const command = @import("command.zig");
const Stream = @import("stream.zig").Stream;
const StreamID = @import("stream.zig").StreamID;
const StreamRecord = @import("stream.zig").Record;
const CountingReader = @import("reader.zig").CountingReader;
const rdb = @import("rdb.zig");
const testing = std.testing;

pub const Server = struct {
    const ValueData = union(enum) {
        string: []const u8,
        stream: *Stream,
        number: i64,

        pub fn deinit(self: ValueData, allocator: std.mem.Allocator) void {
            switch (self) {
                .string => |s| allocator.free(s),
                .stream => |s| s.deinit(),
                .number => {},
            }
        }
    };

    const Value = struct {
        data: ValueData,
        expires: u64,
    };

    pub const Config = struct {
        pub const ReplicaOf = struct {
            host: []const u8,
            port: u16,

            pub fn deinit(self: ReplicaOf, allocator: std.mem.Allocator) void {
                allocator.free(self.host);
            }

            pub fn parse(allocator: std.mem.Allocator, input: []const u8) !ReplicaOf {
                const space = std.mem.indexOf(u8, input, " ") orelse {
                    return error.InvalidReplicaOf;
                };
                const port = try std.fmt.parseInt(u16, input[space + 1 ..], 10);
                return .{
                    .host = try allocator.dupe(u8, input[0..space]),
                    .port = port,
                };
            }

            test "parse" {
                const r = try parse(testing.allocator, "localhost 3333");
                defer r.deinit(testing.allocator);
                try testing.expectEqual(r.port, 3333);
                try testing.expectEqualStrings(r.host, "localhost");
            }
        };

        dir: []const u8 = "",
        dbfilename: []const u8 = "",
        port: u16 = 6379,
        replicaof: ?ReplicaOf = null,
        replid: []const u8 = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    };

    pub const Slave = struct {
        conn: std.net.Server.Connection,
        ack: bool,
    };

    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex = .{},
    values: std.StringHashMap(Value),
    config: Config,
    slaves: std.ArrayList(Slave),

    pub fn init(allocator: std.mem.Allocator, config: Config) Server {
        return .{
            .allocator = allocator,
            .values = std.StringHashMap(Value).init(allocator),
            .slaves = std.ArrayList(Slave).init(allocator),
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
        for (self.slaves.items) |slave| {
            slave.conn.stream.close();
        }
        self.slaves.deinit();
    }

    pub fn serve(self: *Server, conn: std.net.Server.Connection) void {
        var slave = false;
        defer {
            if (!slave) {
                conn.stream.close();
            }
        }
        const r = conn.stream.reader().any();
        const w = conn.stream.writer().any();
        while (true) {
            self.next(r, w) catch |err| switch (err) {
                error.EndOfStream => return,
                error.RegisterSlave => {
                    self.mutex.lock();
                    defer self.mutex.unlock();
                    self.slaves.append(.{ .conn = conn, .ack = true }) catch |err2| {
                        std.debug.print("unable to register slave: {s}\n", .{@errorName(err2)});
                        return;
                    };
                    slave = true;
                    return;
                },
                else => {
                    std.debug.print("next error: {s}\n", .{@errorName(err)});
                    return;
                },
            };
        }
    }

    pub fn replication(self: *Server) !void {
        const r = self.config.replicaof orelse return;
        while (true) {
            self.replicate(r) catch |err| {
                std.debug.print("REPLICATION: ERR: {s}\n", .{@errorName(err)});
            };
            std.time.sleep(5 * std.time.ns_per_s);
        }
    }

    pub fn replicate(self: *Server, r: Config.ReplicaOf) !void {
        const conn = try std.net.tcpConnectToHost(self.allocator, r.host, r.port);
        defer conn.close();
        const writer = conn.writer().any();
        const reader = conn.reader().any();

        // PING
        try resp.Value.writeArrayOpen(writer, 1);
        try resp.Value.write(.{ .string = "PING" }, writer);
        const ping_res = try resp.Value.read(self.allocator, reader);
        defer ping_res.deinit(self.allocator);

        // REPLCONF port
        try resp.Value.writeArrayOpen(writer, 3);
        try resp.Value.write(.{ .string = "REPLCONF" }, writer);
        try resp.Value.write(.{ .string = "listening-port" }, writer);
        try resp.Value.writeIntString(writer, self.config.port);
        const replconf_res1 = try resp.Value.read(self.allocator, reader);
        defer replconf_res1.deinit(self.allocator);

        // REPLCONF capabilities
        try resp.Value.writeArrayOpen(writer, 3);
        try resp.Value.write(.{ .string = "REPLCONF" }, writer);
        try resp.Value.write(.{ .string = "capa" }, writer);
        try resp.Value.write(.{ .string = "psync2" }, writer);
        const replconf_res2 = try resp.Value.read(self.allocator, reader);
        defer replconf_res2.deinit(self.allocator);

        // PSYNC
        try resp.Value.writeArrayOpen(writer, 3);
        try resp.Value.write(.{ .string = "PSYNC" }, writer);
        try resp.Value.write(.{ .string = "?" }, writer);
        try resp.Value.write(.{ .string = "-1" }, writer);
        const psync_res = try resp.Value.read(self.allocator, reader);
        defer psync_res.deinit(self.allocator);

        // SKIP THE RDB SNAPSHOT
        const line = try reader.readUntilDelimiterAlloc(self.allocator, '\n', 1 << 20);
        defer self.allocator.free(line);
        var snapshot = try rdb.File.read(self.allocator, reader);
        snapshot.deinit();

        var n_processed: u64 = 0;
        var counter = CountingReader.init(reader);

        // PROCESS COMMANDS
        while (true) {
            const req = try resp.Request.read(self.allocator, counter.any());
            defer req.deinit();
            std.debug.print("request: {s}, args: {any}\n", .{ req.name, req.args });

            // HANDLE ACK
            if (req.is("REPLCONF") and req.at(0).is("GETACK") and req.at(1).is("*")) {
                try resp.Value.writeArrayOpen(writer, 3);
                try resp.Value.write(.{ .string = "REPLCONF" }, writer);
                try resp.Value.write(.{ .string = "ACK" }, writer);
                try resp.Value.writeIntString(writer, n_processed);
                continue;
            }

            // FORWARD
            var fbs = std.io.fixedBufferStream(&[_]u8{});
            try self.handle(req, fbs.reader().any(), std.io.null_writer.any());

            // keep track of how many bytes we've processed
            n_processed += counter.bytes_read;
            counter.bytes_read = 0;
        }
    }

    pub fn next(self: *Server, r: std.io.AnyReader, w: std.io.AnyWriter) !void {
        const req = try resp.Request.read(self.allocator, r);
        defer req.deinit();
        std.debug.print("request: {s}, args: {any}\n", .{ req.name, req.args });

        try self.publish(req);
        self.handle(req, r, w) catch |err| {
            if (err == error.RegisterSlave) {
                return err;
            }
            try resp.Value.writeErr(w, "ERR: failed to process request {s}", .{@errorName(err)});
        };
    }

    fn publish(self: *Server, req: resp.Request) !void {
        if (!req.is("SET")) {
            return;
        }
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.slaves.items) |*slave| {
            const writer = slave.conn.stream.writer().any();
            try req.write(writer);
            slave.ack = false;
        }
    }

    fn handle(self: *Server, req: resp.Request, r: std.io.AnyReader, w: std.io.AnyWriter) !void {
        if (req.is("COMMAND")) return self.onCommand(w);
        if (req.is("PING")) return self.onPing(w);
        if (req.is("ECHO")) return self.onEcho(w, req.args);
        if (req.is("SET")) return self.onSet(w, req.args);
        if (req.is("GET")) return self.onGet(w, req.args);
        if (req.is("DUMP")) return self.onDump(w);
        if (req.is("CONFIG")) return self.onConfig(w, req.args);
        if (req.is("KEYS")) return self.onKeys(w, req.args);
        if (req.is("TYPE")) return self.onType(w, req.args);
        if (req.is("XADD")) return self.onXAdd(w, req.args);
        if (req.is("XRANGE")) return self.onXRange(w, req.args);
        if (req.is("XREAD")) return self.onXRead(w, req.args);
        if (req.is("INCR")) return self.onIncr(w, req.args);
        if (req.is("MULTI")) return self.onMulti(r, w, req.args);
        if (req.is("EXEC")) return self.onExec(w);
        if (req.is("DISCARD")) return self.onDiscard(w);
        if (req.is("INFO")) return self.onInfo(w, req.args);
        if (req.is("REPLCONF")) return self.onReplConf(w);
        if (req.is("PSYNC")) return self.onPsync(w);
        if (req.is("WAIT")) return self.onWait(w, req.args);
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
                .data = .{ .string = cmd.value.string },
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
        const data = self.getValueData(key) orelse {
            try resp.Value.write(.null_string, w);
            return;
        };
        switch (data.*) {
            .string => |s| {
                return resp.Value.write(.{ .string = s }, w);
            },
            .number => |num| {
                var buf: [32]u8 = undefined;
                const n = std.fmt.formatIntBuf(&buf, num, 10, .lower, .{});
                return resp.Value.write(.{ .string = buf[0..n] }, w);
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
                .string => |str| std.debug.print("STRING {s} = {any}\n", .{ key, str }),
                .number => |num| std.debug.print("NUMBER {s} = {d}\n", .{ key, num }),
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

        if (std.mem.eql(u8, name, "port")) {
            var config = [_]resp.Value{
                .{ .string = name },
                .{ .integer = self.config.port },
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
                .string, .number => {
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

    fn onIncr(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        if (args.len != 1 or args[0] != .string) {
            return error.InvalidArgs;
        }
        self.mutex.lock();
        defer self.mutex.unlock();
        const key = args[0].string;
        const data = self.getValueData(key) orelse {
            const key_dup = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(key_dup);
            try self.values.put(key_dup, .{ .expires = 0, .data = .{ .number = 1 } });
            try resp.Value.write(.{ .integer = 1 }, w);
            return;
        };
        // coerse the value to a number
        switch (data.*) {
            .stream => return resp.Value.writeErr(w, "ERR value is not an integer or out of range", .{}),
            .string => |s| {
                const num = std.fmt.parseInt(i64, s, 10) catch {
                    return resp.Value.writeErr(w, "ERR value is not an integer or out of range", .{});
                };
                data.* = .{ .number = num };
                self.allocator.free(s);
            },
            .number => {},
        }
        data.number += 1;
        try resp.Value.write(.{ .integer = data.number }, w);
    }

    fn onMulti(self: *Server, r: std.io.AnyReader, w: std.io.AnyWriter, args: []resp.Value) !void {
        if (args.len != 0) {
            return error.InvalidArgs;
        }
        try resp.Value.write(.{ .simple = "OK" }, w);
        var reqs = std.ArrayList(resp.Request).init(self.allocator);
        defer {
            for (reqs.items) |req| {
                req.deinit();
            }
            reqs.deinit();
        }
        while (true) {
            const req = try resp.Request.read(self.allocator, r);
            if (req.is("DISCARD")) {
                req.deinit();
                try resp.Value.write(.{ .simple = "OK" }, w);
                return;
            }
            if (req.is("EXEC")) {
                try resp.Value.writeArrayOpen(w, reqs.items.len);
                defer req.deinit();
                for (reqs.items) |req2| {
                    try self.handle(req2, r, w);
                }
                return;
            }
            errdefer req.deinit();
            try reqs.append(req);
            try resp.Value.write(.{ .simple = "QUEUED" }, w);
        }
    }

    fn onExec(_: *Server, w: std.io.AnyWriter) !void {
        try resp.Value.writeErr(w, "ERR EXEC without MULTI", .{});
    }

    fn onDiscard(_: *Server, w: std.io.AnyWriter) !void {
        try resp.Value.writeErr(w, "ERR DISCARD without MULTI", .{});
    }

    fn onInfo(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        if (args.len != 1 or args[0] != .string) {
            return error.InvalidArgs;
        }
        if (!std.mem.eql(u8, args[0].string, "replication")) {
            return error.InvalidArgs;
        }
        const format =
            \\# Replication
            \\role:{s}
            \\master_replid:{s}
            \\master_repl_offset:0
        ;
        const role = if (self.config.replicaof != null) "slave" else "master";
        const info = try std.fmt.allocPrint(self.allocator, format, .{ role, self.config.replid });
        defer self.allocator.free(info);
        try resp.Value.write(.{ .string = info }, w);
    }

    fn onReplConf(_: *Server, w: std.io.AnyWriter) !void {
        try resp.Value.write(.{ .simple = "OK" }, w);
    }

    fn onPsync(self: *Server, w: std.io.AnyWriter) !void {
        const res = try std.fmt.allocPrint(self.allocator, "FULLRESYNC {s} 0", .{self.config.replid});
        defer self.allocator.free(res);
        try resp.Value.write(.{ .simple = res }, w);

        // SEND RDB
        try w.print("${d}\r\n", .{rdb.EMPTY.len});
        try w.writeAll(&rdb.EMPTY);

        // signal to break out of the request/response loop
        // and use this connection for replication
        return error.RegisterSlave;
    }

    fn onWait(self: *Server, w: std.io.AnyWriter, args: []resp.Value) !void {
        _ = args;
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.slaves.items) |*slave| {
            if (slave.ack) {
                continue;
            }
            const writer = slave.conn.stream.writer().any();
            try resp.Value.writeArrayOpen(writer, 3);
            try resp.Value.write(.{ .string = "REPLCONF" }, writer);
            try resp.Value.write(.{ .string = "GETACK" }, writer);
            try resp.Value.write(.{ .string = "*" }, writer);
        }

        for (self.slaves.items) |*slave| {
            if (slave.ack) {
                continue;
            }
            const reader = slave.conn.stream.reader().any();
            const getack_res = try resp.Value.read(self.allocator, reader);
            defer getack_res.deinit(self.allocator);
            slave.ack = true;
        }

        try resp.Value.write(.{ .integer = @intCast(self.slaves.items.len) }, w);
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
            const stream = try self.getStream(op.key.string, false);
            try streams.append(stream);
            stream.mutex.lock();

            // figure out where we're starting
            if (op.latest) {
                op.start = stream.last;
            }
            op.start.sequence += 1;

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

    fn getValueData(self: *Server, key: []const u8) ?*ValueData {
        const entry = self.values.getEntry(key) orelse {
            return null;
        };
        const value: *Value = entry.value_ptr;
        if (value.expires > 0 and std.time.milliTimestamp() > value.expires) {
            // TODO: de-allocate ??
            self.values.removeByPtr(entry.key_ptr);
            return null;
        }
        return &value.data;
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
