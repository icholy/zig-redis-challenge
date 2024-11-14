const std = @import("std");

pub const CountingReader = struct {
    child_reader: std.io.AnyReader,
    bytes_read: u64,

    pub fn init(reader: std.io.AnyReader) CountingReader {
        return .{
            .child_reader = reader,
            .bytes_read = 0,
        };
    }

    pub fn read(self: *CountingReader, buf: []u8) !usize {
        const n = try self.child_reader.read(buf);
        self.bytes_read += n;
        return n;
    }

    pub fn any(self: *CountingReader) std.io.AnyReader {
        return .{ .readFn = readFn, .context = self };
    }

    fn readFn(ctx: *const anyopaque, buf: []u8) anyerror!usize {
        const self: *CountingReader = @ptrCast(@alignCast(@constCast(ctx)));
        return self.read(buf);
    }
};

pub const DebugReader = struct {
    child_reader: std.io.AnyReader,

    pub fn init(reader: std.io.AnyReader) DebugReader {
        return .{
            .child_reader = reader,
        };
    }

    pub fn read(self: *DebugReader, buf: []u8) !usize {
        const n = try self.child_reader.read(buf);
        std.debug.print("DebugReader: read: {s}\n", .{buf[0..n]});
        return n;
    }

    pub fn any(self: *DebugReader) std.io.AnyReader {
        return .{ .readFn = readFn, .context = self };
    }

    fn readFn(ctx: *const anyopaque, buf: []u8) anyerror!usize {
        const self: *DebugReader = @ptrCast(@alignCast(@constCast(ctx)));
        return self.read(buf);
    }
};

pub const DebugWriter = struct {
    child_writer: std.io.AnyWriter,

    pub fn init(writer: std.io.AnyWriter) DebugWriter {
        return .{
            .child_writer = writer,
        };
    }

    pub fn write(self: *DebugWriter, buf: []const u8) !usize {
        std.debug.print("DebugWriter: write: '{s}'\n", .{buf});
        return self.child_writer.write(buf);
    }

    pub fn any(self: *DebugWriter) std.io.AnyWriter {
        return .{ .writeFn = writeFn, .context = self };
    }

    fn writeFn(ctx: *const anyopaque, buf: []const u8) anyerror!usize {
        const self: *DebugWriter = @ptrCast(@alignCast(@constCast(ctx)));
        return self.write(buf);
    }
};
