const std = @import("std");

fn countScalar(comptime T: type, haystack: []const T, needle: T) usize {
    var i: usize = 0;
    var found: usize = 0;

    while (std.mem.indexOfScalarPos(T, haystack, i, needle)) |idx| {
        i = idx + 1;
        found += 1;
    }

    return found;
}

// const columnTypePriority = [_]type {
//     void,
//     i8,
//     i16,
//     i32,
//     i64,
//     f32,
//     []const u8,
// };

const ColumnType = enum {
    void,
    i8,
    i16,
    i32,
    i64,
    f32,
    string,
};

const ColumnData = struct {
    name: []const u8,
    type: ColumnType,
};

fn parseRow(line: []const u8, delim: []const u8, columnData: []ColumnData) !void
{
    var columnIt = std.mem.split(u8, line, delim);
    for (columnData) |cd| {
        _ = cd;
        _ = columnIt;
    }
}

pub const CsvFileParserAuto = struct {
    allocator: std.mem.Allocator,
    delim: []const u8,

    const Self = @This();

    pub fn init(delim: []const u8, allocator: std.mem.Allocator) Self
    {
        return Self {
            .allocator = allocator,
            .delim = delim,
        };
    }

    pub fn deinit(self: *Self) void
    {
        _ = self;
    }

    pub fn parse(self: *Self, filePath: []const u8) !void
    {
        var arenaAllocator = std.heap.ArenaAllocator.init(self.allocator);
        defer arenaAllocator.deinit();
        const tempAllocator = arenaAllocator.allocator();

        const cwd = std.fs.cwd();
        const file = cwd.openFile(filePath, .{}) catch |err| {
            std.log.err("Error \"{}\" when opening file path \"{s}\"", .{err, filePath});
            return err;
        };

        var buf = try self.allocator.alloc(u8, 16 * 1024);
        var lineBuf = std.ArrayList(u8).init(tempAllocator);
        var totalBytes: usize = 0;
        var rows: usize = 0;
        var header = true;
        var columnData = std.ArrayList(ColumnData).init(tempAllocator);
        while (true) {
            const numBytes = try file.read(buf);
            if (numBytes == 0) {
                break;
            }
            totalBytes += numBytes;

            const bytes = buf[0..numBytes];
            var remaining = bytes;
            while (true) {
                if (std.mem.indexOfScalar(u8, remaining, '\n')) |i| {
                    defer {
                        lineBuf.clearRetainingCapacity();
                        remaining = remaining[i+1..];
                    }

                    var line = blk: {
                        if (lineBuf.items.len > 0) {
                            try lineBuf.appendSlice(remaining[0..i]);
                            break :blk lineBuf.items;
                        } else {
                            break :blk remaining[0..i];
                        }
                    };
                    if (line.len > 0 and line[line.len - 1] == '\r') {
                        line = line[0..line.len - 1];
                    }

                    if (header) {
                        header = false;

                        var columnIt = std.mem.split(u8, line, self.delim);
                        while (columnIt.next()) |c| {
                            try columnData.append(ColumnData {
                                .name = try tempAllocator.dupe(u8, c),
                                .type = .void,
                            });
                        }
                    } else {
                        try parseRow(line, self.delim, columnData.items);
                        rows += 1;
                    }
                } else {
                    if (remaining.len > 0) {
                        try lineBuf.appendSlice(remaining);
                    }
                    break;
                }
            }
        }

        if (lineBuf.items.len > 0) {
            try parseRow(lineBuf.items, self.delim, columnData.items);
            rows += 1;
        }

        std.debug.print(
            "Read {} MB file, {} rows, {} columns\n",
            .{totalBytes / 1024 / 1024, rows, columnData.items.len}
        );

        // TODO we can try std.ArrayList instead of having to scan the file twice for row count.
        // Just gotta measure what's faster (will vary based on disk speed, though maybe we
        // wanna minimize disk IO because that has higher potential to be slow ??).
        // if (rows > 0) {
        //     self.rows = try self.allocator.alloc(RowType, rows);
        // }

        // // var lineBuf = std.ArrayList(u8).init(tempAllocator);
        // var header = true;
        // var rowIndex: usize = 0;
        // try file.seekTo(0);
        // var leftover = false;
        // while (true) {
        //     const numBytes = try file.read(buf);
        //     if (numBytes == 0) {
        //         break;
        //     }

        //     const bytes = buf[0..numBytes];
        //     var remaining = bytes;
        //     while (true) {
        //         if (std.mem.indexOfScalar(u8, remaining, '\n')) |i| {
        //             defer remaining = remaining[i+1..];

        //             if (header) {
        //                 header = false;
        //                 continue;
        //             }

        //             if (leftover) {
        //                 leftover = false;
        //                 // TODO special handling
        //                 continue;
        //             }

        //             var line = remaining[0..i];
        //             if (line.len > 0 and line[line.len - 1] == '\r') {
        //                 line = line[0..line.len - 1];
        //             }
        //             if (rowIndex >= self.rows.len) {
        //                 return error.TooManyRows;
        //             }
        //             try parseCsvLine(line, self.delim, &self.rows[rowIndex]);
        //             rowIndex += 1;
        //         } else {
        //             _ = tempAllocator;
        //             if (remaining.len > 0) {
        //                 leftover = true;
        //             }
        //             break;
        //         }
        //     }
        // }
    }
};

pub fn CsvFileParser(comptime RowType: type) type
{
    const T = struct {
        allocator: std.mem.Allocator,
        delim: []const u8,
        rows: []RowType,

        const Self = @This();

        fn parseCsvLine(line: []const u8, delim: []const u8, row: *RowType) !void
        {
            var itDelim = std.mem.split(u8, line, delim);
            inline for (@typeInfo(RowType).Struct.fields) |f| {
                const valueString = itDelim.next() orelse blk: {
                    if (@typeInfo(f.type) == .Optional) {
                        if (@typeInfo(f.type).Optional.child != void) {
                            return error.UnsupportedOptionalType;
                        }
                        break :blk "";
                    }
                    return error.MissingField;
                };
                switch (@typeInfo(f.type)) {
                    .Float => {
                        var value: f.type = 0;
                        if (valueString.len > 0) {
                            value = std.fmt.parseFloat(f.type, valueString) catch |err| {
                                std.log.err(
                                    "error \"{}\" when parsing field \"{s}\" in row \"{s}\", field string \"{s}\"",
                                    .{err, f.name, line, valueString}
                                );
                                return error.BadField;
                            };
                        }
                        @field(row, f.name) = value;
                    },
                    .Int => {
                        var value: f.type = 0;
                        if (valueString.len > 0) {
                            value = std.fmt.parseInt(f.type, valueString, 10) catch |err| {
                                std.log.err(
                                    "error \"{}\" when parsing field \"{s}\" in row \"{s}\", field string \"{s}\"",
                                    .{err, f.name, line, valueString}
                                );
                                return error.BadField;
                            };
                        }
                        @field(row, f.name) = value;
                    },
                    .Pointer => |info| switch (info.size) {
                        .Slice => {
                            @field(row, f.name) = "";
                        },
                        else => return error.UnexpectedRowType,
                    },
                    .Void => {}, // field value not captured
                    .Optional => {},
                    else => return error.UnexpectedRowType,
                }
            }

            const rest = itDelim.rest();
            if (rest.len > 0) {
                std.log.err(
                    "row type incomplete, row \"{s}\" still has \"{s}\" left to parse",
                    .{line, rest}
                );
                return error.RowTypeIncomplete;
            }
        }

        pub fn init(delim: []const u8, allocator: std.mem.Allocator) Self
        {
            return Self {
                .allocator = allocator,
                .delim = delim,
                .rows = &.{},
            };
        }

        pub fn deinit(self: *Self) void
        {
            if (self.rows.len > 0) {
                self.allocator.free(self.rows);
            }
        }

        pub fn parse(self: *Self, filePath: []const u8) !void
        {
            var arenaAllocator = std.heap.ArenaAllocator.init(self.allocator);
            defer arenaAllocator.deinit();
            const tempAllocator = arenaAllocator.allocator();

            const cwd = std.fs.cwd();
            const file = cwd.openFile(filePath, .{}) catch |err| {
                std.log.err("Error \"{}\" when opening file path \"{s}\"", .{err, filePath});
                return err;
            };

            var buf = try self.allocator.alloc(u8, 16 * 1024);
            var totalBytes: usize = 0;
            var rows: usize = 0;
            while (true) {
                const numBytes = try file.read(buf);
                if (numBytes == 0) {
                    break;
                }
                totalBytes += numBytes;
                rows += countScalar(u8, buf, '\n');
            }

            std.debug.print("Read {} MB file, {} rows\n", .{totalBytes / 1024 / 1024, rows});

            // TODO we can try std.ArrayList instead of having to scan the file twice for row count.
            // Just gotta measure what's faster (will vary based on disk speed, though maybe we
            // wanna minimize disk IO because that has higher potential to be slow ??).
            if (rows > 0) {
                self.rows = try self.allocator.alloc(RowType, rows);
            }

            // var lineBuf = std.ArrayList(u8).init(tempAllocator);
            var header = true;
            var rowIndex: usize = 0;
            try file.seekTo(0);
            var leftover = false;
            while (true) {
                const numBytes = try file.read(buf);
                if (numBytes == 0) {
                    break;
                }

                const bytes = buf[0..numBytes];
                var remaining = bytes;
                while (true) {
                    if (std.mem.indexOfScalar(u8, remaining, '\n')) |i| {
                        defer remaining = remaining[i+1..];

                        if (header) {
                            header = false;
                            continue;
                        }

                        if (leftover) {
                            leftover = false;
                            // TODO special handling
                            continue;
                        }

                        var line = remaining[0..i];
                        if (line.len > 0 and line[line.len - 1] == '\r') {
                            line = line[0..line.len - 1];
                        }
                        if (rowIndex >= self.rows.len) {
                            return error.TooManyRows;
                        }
                        try parseCsvLine(line, self.delim, &self.rows[rowIndex]);
                        rowIndex += 1;
                    } else {
                        _ = tempAllocator;
                        if (remaining.len > 0) {
                            leftover = true;
                        }
                        break;
                    }
                }
            }
        }
    };
    return T;
}
