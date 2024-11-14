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
