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

const ColumnType = enum(u8) {
    none = 0,
    i8,
    i16,
    i32,
    i64,
    f32,
    string,
};

fn getZigType(comptime columnType: ColumnType) type
{
    return switch (columnType) {
        .none => void,
        .i8 => i8,
        .i16 => i16,
        .i32 => i32,
        .i64 => i64,
        .f32 => f32,
        .string => []const u8,
    };
}

const ColumnMetadata = struct {
    names: [][]const u8,
    types: []ColumnType,
};

const ColumnData = struct {
    name: []const u8, // not great for cache locality
    type: ColumnType,
};

const ColumnValue = union(ColumnType) {
    none: void,
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    f32: f32,
    string: []const u8,
};

fn parseColumn(valueString: []const u8, columnType: *ColumnType) !?ColumnValue
{
    if (valueString.len == 0) {
        return null;
    }

    var ct = columnType.*;
    while (true) : (ct = @intToEnum(ColumnType, @enumToInt(ct) + 1)) {
        switch (ct) {
            .none => {},
            // .i8, .i16, .i32, .i64 => {
            //     const value = std.fmt.parseInt(i64, valueString, 10) catch continue;
            //     columnType.* = ct;
            //     return ColumnValue {
            //         .i64 = value
            //     };
            // },
            .f32 => {
                const value = std.fmt.parseFloat(f32, valueString) catch continue;
                columnType.* = ct;
                return ColumnValue {
                    .f32 = value
                };
            },
            .string => {
                columnType.* = ct;
                return ColumnValue {
                    .string = ""
                };
            },
            inline else => |t| {
                const value = std.fmt.parseInt(getZigType(t), valueString, 10) catch continue;
                columnType.* = ct;
                return @unionInit(ColumnValue, @tagName(t), value);
                // var columnValue: ColumnValue = undefined;
                // return ColumnValue {
                //     value
                // };
            },
        }
    }
}

fn parseRow(
    line: []const u8,
    delim: []const u8,
    columnData: []ColumnData,
    values: []?ColumnValue) !void
{
    std.debug.assert(columnData.len == values.len);

    var columnIt = std.mem.split(u8, line, delim);
    for (columnData) |*cd, i| {
        const valueString = columnIt.next() orelse "";
        values[i] = try parseColumn(valueString, &cd.type);
    }
}

const ParseState = struct {
    fileBuf: [16 * 1024]u8,
    lineBytes: usize,
    lineBuf: [16 * 1024]u8,
};

const LineIterator = struct {
    parseState: *ParseState,

    const Self = @This();

    fn init(parseState: *ParseState) Self
    {
        parseState.lineBytes = 0;
        return Self {
            .parseState = parseState,
        };
    }

    fn next(self: *Self, reader: anytype) ?[]const u8
    {
        while (true) {
            const numBytes = try reader.read(&self.parseState.fileBuf);
            if (numBytes == 0) {
                break;
            }

            const bytes = self.parseState.fileBuf[0..numBytes];
            var remaining = bytes;
            while (true) {
                if (std.mem.indexOfScalar(u8, remaining, '\n')) |i| {
                    defer {
                        remaining = remaining[i+1..];
                    }

                    var line = blk: {
                        if (self.parseState.lineBytes > 0) {
                            const newSize = self.parseState.lineBytes + i;
                            if (newSize > self.parseState.lineBuf.len) {
                                return error.LineTooLong;
                            }
                            std.mem.copy(
                                u8,
                                self.parseState.lineBuf[self.parseState.lineBytes..newSize],
                                remaining[0..i]
                            );
                            self.parseState.lineBytes = 0;
                            break :blk self.parseState.lineBuf[0..newSize];
                        } else {
                            break :blk remaining[0..i];
                        }
                    };
                    if (line.len > 0 and line[line.len - 1] == '\r') {
                        line = line[0..line.len - 1];
                    }
                } else {
                    if (remaining.len > 0) {
                        const newSize = self.parseState.lineBytes + remaining.len;
                        _ = newSize;
                        // if (newSize > 0)
                        // try self.parseState.lineBuf.appendSlice(remaining);
                    }
                    break;
                }
            }
        }
    }
};

fn getNumColumns(filePath: []const u8, parseState: *ParseState) !usize
{
    const cwd = std.fs.cwd();
    const file = cwd.openFile(filePath, .{}) catch |err| {
        std.log.err("Error \"{}\" when opening file path \"{s}\"", .{err, filePath});
        return err;
    };

    _ = file;
    _ = parseState;
}

fn getColumnMetadata(
    filePath: []const u8,
    fileStart: usize,
    fileBytes: usize,
    columnMetadata: *ColumnMetadata) !void
{
    const cwd = std.fs.cwd();
    const file = cwd.openFile(filePath, .{}) catch |err| {
        std.log.err("Error \"{}\" when opening file path \"{s}\"", .{err, filePath});
        return err;
    };

    // var buf = try self.allocator.alloc(u8, 16 * 1024);

    _ = file;
    _ = fileStart;
    _ = fileBytes;
    _ = columnMetadata;
}

pub const CsvFileParserAuto = struct {
    allocator: std.mem.Allocator,
    delim: []const u8,
    columnData: std.ArrayList(ColumnData),
    rows: std.ArrayList([]?ColumnValue),

    const Self = @This();

    pub fn init(delim: []const u8, allocator: std.mem.Allocator) Self
    {
        return Self {
            .allocator = allocator,
            .delim = delim,
            .columnData = std.ArrayList(ColumnData).init(allocator),
            .rows = std.ArrayList([]?ColumnValue).init(allocator),
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
        var valuesBuf = try tempAllocator.alloc(?ColumnValue, 512);
        var totalBytes: usize = 0;
        var header = true;
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
                            try self.columnData.append(ColumnData {
                                .name = try self.allocator.dupe(u8, c),
                                .type = .none,
                            });
                        }

                        if (self.columnData.items.len > valuesBuf.len) {
                            return error.TooManyColumns;
                        }
                    } else {
                        try parseRow(line, self.delim, self.columnData.items, valuesBuf[0..self.columnData.items.len]);
                        // try self.rows.append(values);
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
            try parseRow(lineBuf.items, self.delim, self.columnData.items, valuesBuf[0..self.columnData.items.len]);
            // try self.rows.append(values);
        }

        std.debug.print(
            "Read {} MB file, {} rows, {} columns\n",
            .{totalBytes / 1024 / 1024, self.rows.items.len, self.columnData.items.len}
        );
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
