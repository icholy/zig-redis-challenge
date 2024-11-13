const std = @import("std");

pub fn ieql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) {
        return false;
    }
    for (a, b) |c1, c2| {
        if (std.ascii.toUpper(c1) != std.ascii.toUpper(c2)) {
            return false;
        }
    }
    return true;
}
