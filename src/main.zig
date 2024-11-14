const std = @import("std");
const net = std.net;
const testing = std.testing;
const Server = @import("server.zig").Server;
const rdb = @import("rdb.zig");
const resp = @import("resp.zig");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    var tsa = std.heap.ThreadSafeAllocator{ .child_allocator = gpa.allocator() };
    const allocator = tsa.allocator();

    var config: Server.Config = .{};
    var args = std.process.args();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--dir")) {
            const dir = args.next() orelse {
                std.debug.print("--dir expects value\n", .{});
                std.process.exit(1);
            };
            config.dir = try allocator.dupe(u8, dir);
            continue;
        }
        if (std.mem.eql(u8, arg, "--dbfilename")) {
            const dbfilename = args.next() orelse {
                std.debug.print("--dbfilename expects value\n", .{});
                std.process.exit(1);
            };
            config.dbfilename = try allocator.dupe(u8, dbfilename);
            continue;
        }
    }

    var server = Server.init(allocator, config);
    defer server.deinit();

    if (config.dbfilename.len != 0) {
        var dir = std.fs.cwd();
        if (config.dir.len != 0) {
            dir = try dir.openDir(config.dir, .{});
        }
        const dbfile = dir.openFile(config.dbfilename, .{}) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (dbfile) |file| {
            defer file.close();
            var db = try rdb.File.read(allocator, file.reader().any());
            defer db.deinit();
            while (db.pairs.popOrNull()) |pair| {
                errdefer pair.deinit(allocator);
                try server.values.put(pair.key, .{
                    .expires = pair.expires,
                    .data = .{ .string = pair.value },
                });
            }
        }
    }

    const addr = try net.Address.resolveIp("127.0.0.1", 6379);
    var listener = try addr.listen(.{ .reuse_address = true });
    defer listener.deinit();

    while (true) {
        const conn = try listener.accept();
        std.debug.print("accepted new connection\n", .{});
        var handle = try std.Thread.spawn(.{}, Server.serve, .{ &server, conn });
        handle.detach();
    }
}
