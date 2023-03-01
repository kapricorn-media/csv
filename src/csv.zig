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

    const Self = @This();

    fn init(numColumns: usize, allocator: std.mem.Allocator) !Self
    {
        var names = try allocator.alloc([]const u8, numColumns);
        for (names) |*name| {
            name.* = "";
        }
        var types = try allocator.alloc(ColumnType, numColumns);
        for (types) |*columnType| {
            columnType.* = .none;
        }
        return Self {
            .names = names,
            .types = types,
        };
    }
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
            },
        }
    }
}

const ParseState = struct {
    fileBuf: [16 * 1024]u8,
    fileSlice: []const u8,
    lineBytes: usize,
    lineBuf: [16 * 1024]u8,
};

const LineIterator = struct {
    parseState: *ParseState,
    readBytes: usize,
    maxBytes: usize,

    const Self = @This();

    fn init(parseState: *ParseState, maxBytes: usize) Self
    {
        parseState.lineBytes = 0;
        parseState.fileSlice.len = 0;
        return Self {
            .parseState = parseState,
            .readBytes = 0,
            .maxBytes = maxBytes,
        };
    }

    fn next(self: *Self, reader: anytype) !?[]const u8
    {
        while (true) {
            if (self.parseState.fileSlice.len == 0) {
                // TODO restrict based on maxBytes
                const numBytes = try reader.read(&self.parseState.fileBuf);
                if (numBytes == 0) {
                    break;
                }
                defer self.readBytes += numBytes;

                self.parseState.fileSlice = self.parseState.fileBuf[0..numBytes];
            }

            while (true) {
                if (std.mem.indexOfScalar(u8, self.parseState.fileSlice, '\n')) |i| {
                    var line = blk: {
                        if (self.parseState.lineBytes > 0) {
                            const newSize = self.parseState.lineBytes + i;
                            if (newSize > self.parseState.lineBuf.len) {
                                return error.LineTooLong;
                            }
                            std.mem.copy(
                                u8,
                                self.parseState.lineBuf[self.parseState.lineBytes..newSize],
                                self.parseState.fileSlice[0..i]
                            );
                            self.parseState.lineBytes = 0;
                            break :blk self.parseState.lineBuf[0..newSize];
                        } else {
                            break :blk self.parseState.fileSlice[0..i];
                        }
                    };
                    if (line.len > 0 and line[line.len - 1] == '\r') {
                        line = line[0..line.len - 1];
                    }
                    self.parseState.fileSlice = self.parseState.fileSlice[i+1..];
                    return line;
                } else {
                    if (self.parseState.fileSlice.len > 0) {
                        const newSize = self.parseState.lineBytes + self.parseState.fileSlice.len;
                        if (newSize > self.parseState.lineBuf.len) {
                            return error.LineTooLong;
                        }
                        std.mem.copy(
                            u8,
                            self.parseState.lineBuf[self.parseState.lineBytes..newSize],
                            self.parseState.fileSlice
                        );
                        self.parseState.lineBytes = newSize;
                    }
                    self.parseState.fileSlice.len = 0;
                    break;
                }
            }
        }

        if (self.parseState.lineBytes > 0) {
            const lineBytes = self.parseState.lineBytes;
            self.parseState.lineBytes = 0;
            return self.parseState.lineBuf[0..lineBytes];
        } else {
            return null;
        }
    }
};

const CsvMetadata = struct {
    fileSize: usize,
    numColumns: usize,
};

fn getCsvMetadata(filePath: []const u8, delim: []const u8, parseState: *ParseState) !CsvMetadata
{
    const cwd = std.fs.cwd();
    var file = cwd.openFile(filePath, .{}) catch |err| {
        std.log.err("Error \"{}\" when opening file path \"{s}\"", .{err, filePath});
        return err;
    };
    defer file.close();
    var stat = try file.stat();

    var lineIt = LineIterator.init(parseState, 0);
    const header = try lineIt.next(file.reader()) orelse return error.NoCsvHeader;
    return CsvMetadata {
        .fileSize = stat.size,
        .numColumns = std.mem.count(u8, header, delim) + 1,
    };
}

fn getColumnMetadata(
    filePath: []const u8,
    fileStart: usize,
    fileBytes: usize,
    delim: []const u8,
    parseState: *ParseState,
    columnMetadata: *ColumnMetadata,
    stringAllocator: std.mem.Allocator) !void
{
    const cwd = std.fs.cwd();
    var file = cwd.openFile(filePath, .{}) catch |err| {
        std.log.err("Error \"{}\" when opening file path \"{s}\"", .{err, filePath});
        return err;
    };
    defer file.close();
    try file.seekTo(fileStart);

    var lineIt = LineIterator.init(parseState, fileBytes);
    const fileReader = file.reader();
    var header = true;
    while (try lineIt.next(fileReader)) |line| {
        if (header) {
            header = false;
            var delimIt = std.mem.split(u8, line, delim);
            for (columnMetadata.names) |_, i| {
                const columnName = delimIt.next() orelse return error.MissingColumnName;
                columnMetadata.names[i] = try stringAllocator.dupe(u8, columnName);
            }
            if (delimIt.rest().len > 0) {
                return error.ExtraHeaderData;
            }
            continue;
        }

        var delimIt = std.mem.split(u8, line, delim);
        for (columnMetadata.types) |_, i| {
            const valueString = delimIt.next() orelse "";
            _ = try parseColumn(valueString, &columnMetadata.types[i]);
        }
    }
}

pub const CsvFileParserAuto = struct {
    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    delim: []const u8,
    csvMetadata: CsvMetadata,
    columnMetadata: ColumnMetadata,

    const Self = @This();

    pub fn init(delim: []const u8, allocator: std.mem.Allocator) Self
    {
        return Self {
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .delim = delim,
            .csvMetadata = undefined,
            .columnMetadata = undefined,
        };
    }

    pub fn deinit(self: *Self) void
    {
        self.arena.deinit();
    }

    pub fn parse(self: *Self, filePath: []const u8) !void
    {
        const arenaAllocator = self.arena.allocator();
        var tempArena = std.heap.ArenaAllocator.init(self.allocator);
        defer tempArena.deinit();
        const tempAllocator = tempArena.allocator();

        var parseState = try tempAllocator.create(ParseState);
        self.csvMetadata = try getCsvMetadata(filePath, self.delim, parseState);
        self.columnMetadata = try ColumnMetadata.init(self.csvMetadata.numColumns, arenaAllocator);
        try getColumnMetadata(filePath, 0, self.csvMetadata.fileSize, self.delim, parseState, &self.columnMetadata, arenaAllocator);
        // for (columnMetadata.names) |_, i| {
        //     std.debug.print("{s}: {}\n", .{columnMetadata.names[i], columnMetadata.types[i]});
        // }

        // const cwd = std.fs.cwd();
        // const file = cwd.openFile(filePath, .{}) catch |err| {
        //     std.log.err("Error \"{}\" when opening file path \"{s}\"", .{err, filePath});
        //     return err;
        // };

        // var buf = try self.allocator.alloc(u8, 16 * 1024);
        // var lineBuf = std.ArrayList(u8).init(tempAllocator);
        // var valuesBuf = try tempAllocator.alloc(?ColumnValue, 512);
        // var totalBytes: usize = 0;
        // var header = true;
        // while (true) {
        //     const numBytes = try file.read(buf);
        //     if (numBytes == 0) {
        //         break;
        //     }
        //     totalBytes += numBytes;

        //     const bytes = buf[0..numBytes];
        //     var remaining = bytes;
        //     while (true) {
        //         if (std.mem.indexOfScalar(u8, remaining, '\n')) |i| {
        //             defer {
        //                 lineBuf.clearRetainingCapacity();
        //                 remaining = remaining[i+1..];
        //             }

        //             var line = blk: {
        //                 if (lineBuf.items.len > 0) {
        //                     try lineBuf.appendSlice(remaining[0..i]);
        //                     break :blk lineBuf.items;
        //                 } else {
        //                     break :blk remaining[0..i];
        //                 }
        //             };
        //             if (line.len > 0 and line[line.len - 1] == '\r') {
        //                 line = line[0..line.len - 1];
        //             }

        //             if (header) {
        //                 header = false;

        //                 var columnIt = std.mem.split(u8, line, self.delim);
        //                 while (columnIt.next()) |c| {
        //                     try self.columnData.append(ColumnData {
        //                         .name = try self.allocator.dupe(u8, c),
        //                         .type = .none,
        //                     });
        //                 }

        //                 if (self.columnData.items.len > valuesBuf.len) {
        //                     return error.TooManyColumns;
        //                 }
        //             } else {
        //                 try parseRow(line, self.delim, self.columnData.items, valuesBuf[0..self.columnData.items.len]);
        //                 // try self.rows.append(values);
        //             }
        //         } else {
        //             if (remaining.len > 0) {
        //                 try lineBuf.appendSlice(remaining);
        //             }
        //             break;
        //         }
        //     }
        // }

        // if (lineBuf.items.len > 0) {
        //     try parseRow(lineBuf.items, self.delim, self.columnData.items, valuesBuf[0..self.columnData.items.len]);
        //     // try self.rows.append(values);
        // }

        // std.debug.print(
        //     "Read {} MB file, {} rows, {} columns\n",
        //     .{totalBytes / 1024 / 1024, self.rows.items.len, self.columnData.items.len}
        // );
    }
};
