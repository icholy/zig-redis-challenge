const std = @import("std");
const testing = std.testing;

pub const SearchIndex = struct {
    value: usize,
    exists: bool,
};

pub fn SortedByteMap(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Entry = struct {
            key: u8,
            value: T,
        };

        entries: std.ArrayList(Entry),

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .entries = std.ArrayList(Entry).init(allocator),
            };
        }

        pub fn deinit(self: Self) void {
            self.entries.deinit();
        }

        pub fn count(self: Self) usize {
            return self.entries.items.len;
        }

        pub fn at(self: Self, index: usize) T {
            return self.entries.items[index].value;
        }

        pub fn get(self: *Self, key: u8) ?T {
            const index = self.search(key);
            if (index.exists) {
                return self.entries.items[index.value].value;
            }
            return null;
        }

        pub fn getPtr(self: *Self, key: u8) ?*T {
            const index = self.search(key);
            if (index.exists) {
                return &self.entries.items[index.value].value;
            }
            return null;
        }

        pub fn put(self: *Self, key: u8, value: T) !?T {
            const index = self.search(key);
            if (index.exists) {
                const prev = self.entries.items[index.value];
                self.entries.items[index.value].value = value;
                return prev.value;
            }
            try self.entries.insert(index.value, .{ .key = key, .value = value });
            return null;
        }

        pub fn remove(self: *Self, key: u8) ?T {
            const index = self.search(key);
            if (index.exists) {
                return self.entries.orderedRemove(index.value).value;
            }
            return null;
        }

        pub fn search(self: Self, key: u8) SearchIndex {
            const items = self.entries.items;
            var low: usize = 0;
            var high: usize = items.len;
            while (low < high) {
                // Avoid overflowing in the midpoint calculation
                const mid = low + (high - low) / 2;
                switch (std.math.order(key, items[mid].key)) {
                    .eq => return .{ .value = mid, .exists = true },
                    .gt => low = mid + 1,
                    .lt => high = mid,
                }
            }
            return .{ .value = low, .exists = false };
        }
    };
}

pub fn RadixTree(comptime T: type) type {
    return struct {
        const ChildrenMap = SortedByteMap(Node);

        const InsertErrors = error{
            NotImplemented,
            OutOfMemory,
        };

        const Node = struct {
            seq: []u8,
            children: ChildrenMap,
            value: ?T = null,

            fn init(allocator: std.mem.Allocator, seq: []const u8, value: ?T) !Node {
                return .{
                    .seq = try allocator.dupe(u8, seq),
                    .children = ChildrenMap.init(allocator),
                    .value = value,
                };
            }

            fn deinit(self: Node, allocator: std.mem.Allocator) void {
                for (self.children.entries.items) |entry| {
                    entry.value.deinit(allocator);
                }
                self.children.deinit();
                allocator.free(self.seq);
            }

            fn insert(self: *Node, allocator: std.mem.Allocator, seq: []const u8, value: T) InsertErrors!void {
                const i = std.mem.indexOfDiff(u8, self.seq, seq) orelse {
                    // case: the new seq is the current node.
                    self.value = value;
                    return;
                };
                // case: the current node is a parent of the new seq.
                if (i == self.seq.len) {
                    if (self.children.getPtr(seq[i])) |node| {
                        try node.insert(allocator, seq[i..], value);
                    } else {
                        var node = try Node.init(allocator, seq[i..], value);
                        errdefer node.deinit(allocator);
                        _ = try self.children.put(seq[i], node);
                    }
                    return;
                }
                // case: the new seq is a parent of the current node.
                if (i == seq.len) {
                    std.mem.copyForwards(u8, self.seq, self.seq[i..]);
                    self.seq = try allocator.realloc(self.seq, self.seq.len - i);
                    var prev = self.*;
                    self.* = try Node.init(allocator, seq, value);
                    errdefer prev.deinit(allocator);
                    _ = try self.children.put(prev.seq[0], prev);
                    return;
                }
                // case: the current node and the new seq share a common parent.
                std.mem.copyForwards(u8, self.seq, self.seq[i..]);
                self.seq = try allocator.realloc(self.seq, self.seq.len - i);
                const prev = self.*;
                var new = try Node.init(allocator, seq[i..], value);
                errdefer new.deinit(allocator);
                self.* = try Node.init(allocator, seq[0..i], null);
                _ = try self.children.put(new.seq[0], new);
                _ = try self.children.put(prev.seq[0], prev);
            }

            fn lookup(self: *Node, seq: []const u8) ?T {
                if (!std.mem.startsWith(u8, seq, self.seq)) {
                    return null;
                }
                if (self.seq.len == seq.len) {
                    return self.value;
                }
                const sub_seq = seq[self.seq.len..];
                const child = self.children.getPtr(sub_seq[0]);
                if (child) |node| {
                    return node.lookup(sub_seq);
                }
                return null;
            }

            fn empty(self: *Node) bool {
                return self.value == null and self.children.entries.items.len == 0;
            }

            fn remove(self: *Node, allocator: std.mem.Allocator, seq: []const u8) !?T {
                var value: ?T = null;
                if (!std.mem.startsWith(u8, seq, self.seq)) {
                    return null;
                }
                if (self.seq.len == seq.len) {
                    value = self.value;
                    self.value = null;
                } else {
                    const sub_seq = seq[self.seq.len..];
                    if (self.children.getPtr(sub_seq[0])) |node| {
                        value = try node.remove(allocator, sub_seq);
                        if (node.empty()) {
                            node.deinit(allocator);
                            const old = self.children.remove(sub_seq[0]);
                            std.debug.assert(old != null);
                        }
                    }
                }
                try self.compress(allocator);
                return value;
            }

            fn compress(self: *Node, allocator: std.mem.Allocator) !void {
                if (self.children.entries.items.len != 1 or self.value != null) return;
                // detach the only child
                var child = self.children.entries.items[0].value;
                const old = self.children.remove(child.seq[0]);
                std.debug.assert(old != null);
                errdefer child.deinit(allocator);
                // prefix the child's seq with ours
                const child_len = child.seq.len;
                child.seq = try allocator.realloc(child.seq, self.seq.len + child.seq.len);
                std.mem.copyBackwards(u8, child.seq[self.seq.len..], child.seq[0..child_len]);
                std.mem.copyForwards(u8, child.seq, self.seq);
                // replace ourselves with the child
                self.deinit(allocator);
                self.* = child;
            }

            fn write(self: Node, w: std.io.AnyWriter, indent: usize) !void {
                for (0..indent) |_| {
                    try w.writeByte(' ');
                }
                if (self.seq.len == 0) {
                    try w.writeByte('*');
                } else {
                    try w.writeAll(self.seq);
                }
                if (self.value) |value| {
                    try w.print(" - {d}", .{value});
                }
                try w.writeAll("\n");
                for (self.children.entries.items) |entry| {
                    try entry.value.write(w, indent + 1);
                }
            }
        };

        pub const Iterator = struct {

            // we use this sentinel to signal that a node
            // should use its itself as the next iterator value.
            const SELF = std.math.maxInt(usize);

            const IteratorNode = struct {
                node: Node,
                next: usize = SELF,
            };

            const IteratorEntry = struct {
                seq: []const u8,
                value: T,
            };

            root: ?Node,
            seq: std.ArrayList(u8),
            stack: std.ArrayList(IteratorNode),
            allocator: std.mem.Allocator,

            pub fn init(allocator: std.mem.Allocator, root: ?Node) !Iterator {
                var it = Iterator{
                    .stack = std.ArrayList(IteratorNode).init(allocator),
                    .seq = std.ArrayList(u8).init(allocator),
                    .allocator = allocator,
                    .root = root,
                };
                if (root) |node| {
                    try it.push(node);
                }
                return it;
            }

            pub fn deinit(self: Iterator) void {
                self.stack.deinit();
                self.seq.deinit();
            }

            fn push(self: *Iterator, node: Node) !void {
                try self.seq.appendSlice(node.seq);
                try self.stack.append(.{ .node = node });
            }

            fn pop(self: *Iterator) void {
                const entry = self.stack.pop();
                self.seq.shrinkRetainingCapacity(self.seq.items.len - entry.node.seq.len);
            }

            fn current(self: *Iterator) *IteratorNode {
                return &self.stack.items[self.stack.items.len - 1];
            }

            pub fn next(self: *Iterator) !?IteratorEntry {
                while (self.stack.items.len > 0) {
                    const entry = self.current();
                    if (entry.next == SELF) {
                        entry.next = 0;
                        if (entry.node.value) |value| {
                            return .{ .seq = self.seq.items, .value = value };
                        }
                    }
                    if (entry.next >= entry.node.children.count()) {
                        self.pop();
                        continue;
                    }
                    try self.push(entry.node.children.at(entry.next));
                    entry.next += 1;
                }
                return null;
            }

            pub fn seek(self: *Iterator, prefix: []const u8) !void {
                self.seq.clearRetainingCapacity();
                self.stack.clearRetainingCapacity();
                if (self.root) |node| {
                    try self.push(node);
                }
                var remaining = prefix;
                while (self.stack.items.len > 0) {
                    const entry = self.current();
                    const node = entry.node;
                    const diff = std.mem.indexOfDiff(u8, node.seq, remaining) orelse {
                        // case: node seq exactly matches prefix.
                        return;
                    };
                    if (diff == prefix.len) {
                        // case: node seq matches prefix.
                        return;
                    }
                    if (diff < node.seq.len) {
                        // case: node seq does not match prefix.
                        self.pop();
                        return;
                    }
                    // case: node matches, but there's more prefix remaining.
                    remaining = remaining[diff..];
                    const index = node.children.search(remaining[0]);
                    entry.next = index.value;
                    if (!index.exists) {
                        if (entry.next >= node.children.count()) {
                            self.pop();
                        }
                        return;
                    }
                    entry.next += 1;
                    try self.push(node.children.at(index.value));
                }
            }
        };

        allocator: std.mem.Allocator,
        root: ?Node,

        fn init(allocator: std.mem.Allocator) RadixTree(T) {
            return .{
                .allocator = allocator,
                .root = null,
            };
        }

        fn deinit(self: *RadixTree(T)) void {
            if (self.root) |*node| {
                node.deinit(self.allocator);
            }
        }

        fn write(self: *RadixTree(T), w: std.io.AnyWriter) !void {
            if (self.root) |*node| {
                try node.write(w, 0);
            }
        }

        fn insert(self: *RadixTree(T), seq: []const u8, value: T) InsertErrors!void {
            if (seq.len == 0) {
                return;
            }
            if (self.root) |*node| {
                try node.insert(self.allocator, seq, value);
            } else {
                self.root = try Node.init(self.allocator, seq, value);
            }
        }

        fn remove(self: *RadixTree(T), seq: []const u8) !?T {
            if (seq.len == 0) {
                return null;
            }
            if (self.root) |*node| {
                const value = try node.remove(self.allocator, seq);
                if (node.empty()) {
                    node.deinit(self.allocator);
                    self.root = null;
                }
                return value;
            }
            return null;
        }

        fn lookup(self: *RadixTree(T), seq: []const u8) ?T {
            if (self.root) |*node| {
                return node.lookup(seq);
            }
            return null;
        }

        fn iterator(self: *RadixTree(T)) !Iterator {
            return Iterator.init(self.allocator, self.root);
        }
    };
}

fn expectTreeEqual(tree: *RadixTree(i64), expected: []const u8) !void {
    var output = std.ArrayList(u8).init(testing.allocator);
    defer output.deinit();
    try tree.write(output.writer().any());
    try testing.expectEqualStrings(expected, output.items);
}

test "RadixTree.init: 0" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    const expected = "";
    try expectTreeEqual(&tree, expected);
}

test "RadixTree.insert: 1" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("bar", 2);
    const expected =
        \\*
        \\ bar - 2
        \\ foo - 1
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree.insert: 2" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("bar", 2);
    const expected =
        \\*
        \\ bar - 2
        \\ foo - 1
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree.insert: 3" {
    var tree = RadixTree(i64).init(testing.allocator);
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

test "RadixTree.insert: 4" {
    var tree = RadixTree(i64).init(testing.allocator);
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

test "RadixTree.insert: 5" {
    var tree = RadixTree(i64).init(testing.allocator);
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

test "RadixTree.insert: 6" {
    var tree = RadixTree(i64).init(testing.allocator);
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

test "RadixTree.remove: 1" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    _ = try tree.remove("foo");
    try expectTreeEqual(&tree, "");
}

test "RadixTree).remove: 2" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("bar", 2);
    _ = try tree.remove("foo");
    const expected =
        \\bar - 2
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree.remove: 3" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("foobar", 2);
    _ = try tree.remove("foobar");
    const expected =
        \\foo - 1
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree.remove: 4" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("f", 2);
    _ = try tree.remove("f");
    const expected =
        \\foo - 1
        \\
    ;
    try expectTreeEqual(&tree, expected);
}

test "RadixTree.lookup: 1" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try testing.expectEqual(1, tree.lookup("foo"));
}

test "RadixTree.lookup: 2" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("f", 2);
    try testing.expectEqual(1, tree.lookup("foo"));
    try testing.expectEqual(2, tree.lookup("f"));
}

fn expectIteratorEqual(it: *RadixTree(i64).Iterator, expected: []const u8) !void {
    var output = std.ArrayList(u8).init(testing.allocator);
    defer output.deinit();
    while (try it.next()) |entry| {
        try output.writer().print("{s} - {d}\n", .{ entry.seq, entry.value });
    }
    try testing.expectEqualStrings(expected, output.items);
}

test "RadixTree.iterator: 1" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    var it = try tree.iterator();
    defer it.deinit();
    const expected = "";
    try expectIteratorEqual(&it, expected);
}

test "RadixTree.iterator: 2" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    var it = try tree.iterator();
    defer it.deinit();
    const expected =
        \\foo - 1
        \\
    ;
    try expectIteratorEqual(&it, expected);
}

test "RadixTree.iterator: 3" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("foo", 1);
    try tree.insert("foobar", 2);
    var it = try tree.iterator();
    defer it.deinit();
    const expected =
        \\foo - 1
        \\foobar - 2
        \\
    ;
    try expectIteratorEqual(&it, expected);
}

test "RadixTree.iterator: 4" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("boobar", 0);
    try tree.insert("b", 2);
    try tree.insert("bof", 6);
    try tree.insert("f", 3);
    try tree.insert("fizz", 1);
    var it = try tree.iterator();
    defer it.deinit();
    const expected =
        \\b - 2
        \\bof - 6
        \\boobar - 0
        \\f - 3
        \\fizz - 1
        \\
    ;
    try expectIteratorEqual(&it, expected);
}

test "RadixTree.iterator.seek: 1" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("a", 0);
    try tree.insert("aa", 0);
    var it = try tree.iterator();
    defer it.deinit();
    try it.seek("aa");
    const expected =
        \\aa - 0
        \\
    ;
    try expectIteratorEqual(&it, expected);
}

test "RadixTree.iterator.seek: 2" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("aaa", 0);
    try tree.insert("bbb", 0);
    try tree.insert("ccc", 0);
    var it = try tree.iterator();
    defer it.deinit();
    try it.seek("bb");
    const expected =
        \\bbb - 0
        \\ccc - 0
        \\
    ;
    try expectIteratorEqual(&it, expected);
}

test "RadixTree.iterator.seek: 3" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("1234", 0);
    try tree.insert("9999", 0);
    try tree.insert("8421", 0);
    try tree.insert("8423", 0);
    try tree.insert("0011", 0);
    try tree.insert("3011", 0);
    var it = try tree.iterator();
    defer it.deinit();
    try it.seek("8422");
    const expected =
        \\8423 - 0
        \\9999 - 0
        \\
    ;
    try expectIteratorEqual(&it, expected);
}

test "RadixTree.iterator.seek: 4" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("111", 0);
    try tree.insert("121", 0);
    try tree.insert("123", 0);
    var it = try tree.iterator();
    defer it.deinit();
    try it.seek("124");
    const expected = "";
    try expectIteratorEqual(&it, expected);
}

test "RadixTree.iterator.seek: 5" {
    var tree = RadixTree(i64).init(testing.allocator);
    defer tree.deinit();
    try tree.insert("a", 0);
    try tree.insert("b", 0);
    try tree.insert("c", 0);
    var it = try tree.iterator();
    defer it.deinit();
    try it.seek("");
    const expected =
        \\a - 0
        \\b - 0
        \\c - 0
        \\
    ;
    try expectIteratorEqual(&it, expected);
    try it.seek("");
    try expectIteratorEqual(&it, expected);
}

test "SortedByteMap.get: 1" {
    var map = SortedByteMap(i64).init(testing.allocator);
    defer map.deinit();
    try map.entries.append(.{ .key = '0', .value = 123 });
    const value = map.get('0');
    try testing.expectEqual(123, value);
}

test "SortedByteMap.put: 1" {
    var map = SortedByteMap(i64).init(testing.allocator);
    defer map.deinit();
    try map.entries.append(.{ .key = '0', .value = 123 });
    _ = try map.put('0', 42);
    const value = map.get('0');
    try testing.expectEqual(42, value);
}

test "SortedByteMap.put: 2" {
    var map = SortedByteMap(i64).init(testing.allocator);
    defer map.deinit();
    _ = try map.put('0', 0);
    _ = try map.put('2', 2);
    _ = try map.put('1', 1);

    const Entry = SortedByteMap(i64).Entry;
    try testing.expectEqualDeep(map.entries.items, &[_]Entry{
        .{ .key = '0', .value = 0 },
        .{ .key = '1', .value = 1 },
        .{ .key = '2', .value = 2 },
    });
}

test "SortedByteMap.remove: 1" {
    var map = SortedByteMap(i64).init(testing.allocator);
    defer map.deinit();

    _ = try map.put('8', 8);
    _ = try map.put('1', 1);
    _ = map.remove('7');
    _ = try map.put('4', 4);
    _ = try map.put('9', 9);
    _ = map.remove('8');

    const Entry = SortedByteMap(i64).Entry;
    try testing.expectEqualDeep(map.entries.items, &[_]Entry{
        .{ .key = '1', .value = 1 },
        .{ .key = '4', .value = 4 },
        .{ .key = '9', .value = 9 },
    });
}
