const std = @import("std");
const testing = std.testing;

const RadixTree = struct {
    const ChildrenMap = std.AutoHashMap(u8, Node);

    const InsertErrors = error{
        NotImplemented,
        OutOfMemory,
    };

    const Node = struct {
        seq: []u8,
        children: ChildrenMap,
        value: ?i64 = null,

        fn deinit(self: *Node, allocator: std.mem.Allocator) void {
            var it = self.children.valueIterator();
            while (it.next()) |node| {
                node.deinit(allocator);
            }
            self.children.deinit();
            allocator.free(self.seq);
        }

        fn insert(self: *Node, allocator: std.mem.Allocator, seq: []const u8, value: i64) InsertErrors!void {
            const index = std.mem.indexOfDiff(u8, self.seq, seq);
            if (index) |i| {
                if (i == self.seq.len) {
                    try self.insertChild(allocator, seq[i..], value);
                    return;
                }
                if (i == seq.len) {
                    var prev = self.*;
                    std.mem.copyForwards(u8, prev.seq, prev.seq[i..]);
                    prev.seq = try allocator.realloc(prev.seq, prev.seq.len - i);
                    self.* = .{
                        .seq = try allocator.dupe(u8, seq),
                        .value = value,
                        .children = ChildrenMap.init(allocator),
                    };
                    errdefer prev.deinit(allocator);
                    try self.children.put(seq[i - 1], prev);
                    return;
                }
                var prev = self.*;
                std.mem.copyForwards(u8, prev.seq, prev.seq[i..]);
                prev.seq = try allocator.realloc(prev.seq, prev.seq.len - i);
                var new = Node{
                    .seq = try allocator.dupe(u8, seq[i..]),
                    .value = value,
                    .children = ChildrenMap.init(allocator),
                };
                errdefer new.deinit(allocator);
                self.* = .{
                    .seq = try allocator.dupe(u8, seq[0..i]),
                    .children = ChildrenMap.init(allocator),
                };
                try self.children.put(new.seq[0], new);
                try self.children.put(prev.seq[0], prev);
            } else {
                self.value = value;
            }
        }

        fn insertChild(self: *Node, allocator: std.mem.Allocator, seq: []const u8, value: i64) InsertErrors!void {
            if (self.children.getPtr(seq[0])) |node| {
                try node.insert(allocator, seq, value);
            } else {
                var node = Node{
                    .seq = try allocator.dupe(u8, seq),
                    .value = value,
                    .children = ChildrenMap.init(allocator),
                };
                errdefer node.deinit(allocator);
                try self.children.put(seq[0], node);
            }
        }
    };

    allocator: std.mem.Allocator,
    children: ChildrenMap,

    fn init(allocator: std.mem.Allocator) RadixTree {
        return .{
            .allocator = allocator,
            .children = ChildrenMap.init(allocator),
        };
    }

    fn deinit(self: *RadixTree) void {
        var it = self.children.valueIterator();
        while (it.next()) |node| {
            node.deinit(self.allocator);
        }
        self.children.deinit();
    }

    fn writeChildren(w: std.io.AnyWriter, children: ChildrenMap, indent: usize) !void {
        var it = children.valueIterator();
        while (it.next()) |node| {
            for (0..indent) |_| {
                try w.writeByte(' ');
            }
            try w.writeAll(node.seq);
            if (node.value) |v| {
                try w.print(" - {d}", .{v});
            }
            try w.writeAll("\n");
            try writeChildren(w, node.children, indent + 1);
        }
    }

    fn write(self: *RadixTree, w: std.io.AnyWriter) !void {
        try writeChildren(w, self.children, 0);
    }

    fn insert(self: *RadixTree, seq: []const u8, value: i64) InsertErrors!void {
        if (seq.len == 0) {
            return;
        }
        if (self.children.getPtr(seq[0])) |node| {
            try node.insert(self.allocator, seq, value);
        } else {
            var node = Node{
                .seq = try self.allocator.dupe(u8, seq),
                .value = value,
                .children = ChildrenMap.init(self.allocator),
            };
            errdefer node.deinit(self.allocator);
            try self.children.put(seq[0], node);
        }
    }

    fn remove(self: *RadixTree, seq: []const u8) !void {
        if (seq.len == 0) {
            return;
        }
        if (self.children.getPtr(seq[0])) |node| {
            if (std.mem.eql(u8, node.seq, seq)) {
                node.deinit(self.allocator);
                _ = self.children.remove(seq[0]);
            } else {
                return error.NotImplemented;
            }
        }
    }
};

fn expectTreeEqual(tree: *RadixTree, expected: []const u8) !void {
    var output = std.ArrayList(u8).init(testing.allocator);
    defer output.deinit();
    try tree.write(output.writer().any());
    try testing.expectEqualStrings(expected, output.items);
}

test "RadixTree: 1" {
    var tree = RadixTree.init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("bar", 2);
    const expected =
        \\bar - 2
        \\foo - 1
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree: 2" {
    var tree = RadixTree.init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("bar", 2);
    const expected =
        \\bar - 2
        \\foo - 1
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree: 3" {
    var tree = RadixTree.init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foobar", 1);
    try tree.insert("foo", 2);
    const expected =
        \\foo - 2
        \\ bar - 1
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree: 4" {
    var tree = RadixTree.init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foobar", 1);
    try tree.insert("foopoo", 2);
    const expected =
        \\foo
        \\ bar - 1
        \\ poo - 2
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree: 5" {
    var tree = RadixTree.init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foobar", 1);
    try tree.insert("foopoo", 2);
    try tree.insert("f", 3);
    const expected =
        \\f - 3
        \\ oo
        \\  bar - 1
        \\  poo - 2
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree: 6" {
    var tree = RadixTree.init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foobar", 1);
    try tree.insert("f", 2);
    try tree.insert("f", 3);
    const expected =
        \\f - 3
        \\ oobar - 1
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree: 7" {
    var tree = RadixTree.init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("bar", 2);
    try tree.remove("foo");
    const expected =
        \\bar - 2
        \\
    ;
    try expectTreeEqual(&tree, expected);
}
