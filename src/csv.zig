const std = @import("std");

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

    const Self = @This();

    fn init(filePath: []const u8, delim: []const u8, parseState: *ParseState) !Self
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
        return Self {
            .fileSize = stat.size,
            .numColumns = std.mem.count(u8, header, delim) + 1,
        };
    }
};

const ColumnType = enum(u8) {
    none = 0,
    i8,
    i16,
    i32,
    i64,
    f32,
    string,
};

pub fn getZigType(comptime columnType: ColumnType) type
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

pub fn getZigTypeSize(columnType: ColumnType) usize
{
    return switch (columnType) {
        .none, .string => 0,
        inline else => |ct| @sizeOf(getZigType(ct)),
    };
}

fn getColumnType(valueString: []const u8, currentType: ColumnType) ColumnType
{
    if (valueString.len == 0) {
        return currentType;
    }

    var ct = currentType;
    while (true) : (ct = @intToEnum(ColumnType, @enumToInt(ct) + 1)) {
        switch (ct) {
            .none => {},
            .f32 => {
                _ = std.fmt.parseFloat(f32, valueString) catch continue;
                return ct;
            },
            .string => {
                return ct;
            },
            inline else => |t| {
                _ = std.fmt.parseInt(getZigType(t), valueString, 10) catch continue;
                return ct;
            },
        }
    }
}

const CsvMetadataExt = struct {
    numRows: usize,
    columnNames: [][]const u8,
    columnTypes: []ColumnType,
    columnOffsets: []usize,

    const Self = @This();

    fn init(
        numColumns: usize,
        filePath: []const u8,
        fileStart: usize,
        fileBytes: usize,
        delim: []const u8,
        parseState: *ParseState,
        allocator: std.mem.Allocator,
        stringAllocator: std.mem.Allocator) !Self
    {
        var self = Self {
            .numRows = 0,
            .columnNames = try allocator.alloc([]const u8, numColumns),
            .columnTypes = try allocator.alloc(ColumnType, numColumns),
            .columnOffsets = try allocator.alloc(usize, numColumns),
        };
        for (self.columnNames) |_, i| {
            self.columnNames[i] = "";
            self.columnTypes[i] = .none;
            self.columnOffsets[i] = 0;
        }

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
                for (self.columnNames) |_, i| {
                    const columnName = delimIt.next() orelse return error.MissingColumnName;
                    self.columnNames[i] = try stringAllocator.dupe(u8, columnName);
                }
                if (delimIt.rest().len > 0) {
                    return error.ExtraHeaderData;
                }
                continue;
            }

            var delimIt = std.mem.split(u8, line, delim);
            for (self.columnTypes) |_, i| {
                const valueString = delimIt.next() orelse "";
                self.columnTypes[i] = getColumnType(valueString, self.columnTypes[i]);
            }
            self.numRows += 1;
        }

        for (self.columnOffsets) |_, i| {
            if (i == 0) {
                self.columnOffsets[i] = 0;
            } else {
                const prevSize = getZigTypeSize(self.columnTypes[i - 1]) * self.numRows;
                self.columnOffsets[i] = self.columnOffsets[i - 1] + prevSize;
            }
        }

        return self;
    }
};

fn getCsvDataSize(metadataExt: CsvMetadataExt) usize
{
    var size: usize = 0;
    for (metadataExt.columnTypes) |columnType| {
        size += getZigTypeSize(columnType);
    }
    size *= metadataExt.numRows;
    return size;
}

const CsvData = struct {
    data: []u8,

    const Self = @This();

    fn init(
        metadataExt: CsvMetadataExt,
        filePath: []const u8,
        fileStart: usize,
        fileBytes: usize,
        delim: []const u8,
        parseState: *ParseState,
        allocator: std.mem.Allocator) !Self
    {
        const size = getCsvDataSize(metadataExt);
        var self = Self {
            .data = try allocator.alloc(u8, size),
        };

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
        var row: usize = 0;
        while (try lineIt.next(fileReader)) |line| {
            if (header) {
                header = false;
                continue;
            }

            var delimIt = std.mem.split(u8, line, delim);
            for (metadataExt.columnTypes) |columnType, i| {
                const valueString = delimIt.next() orelse "";
                self.parseAndSaveValue(metadataExt, valueString, columnType, row, i) catch |err| {
                    std.log.err("{} when parsing valueString \"{s}\"", .{err, valueString});
                    return err;
                };
            }
            row += 1;
        }

        return self;
    }

    fn parseAndSaveValue(
        self: *Self,
        metadataExt: CsvMetadataExt,
        valueString: []const u8,
        columnType: ColumnType,
        row: usize,
        col: usize) !void
    {
        const offsetBase = metadataExt.columnOffsets[col];
        const offset = offsetBase + row * getZigTypeSize(columnType);
        switch (columnType) {
            .none, .string => {},
            inline else => |ct| {
                const zigType = comptime getZigType(ct);
                const value = blk: {
                    if (valueString.len == 0) {
                        break :blk 0;
                    } else {
                        if (zigType == f32) {
                            break :blk try std.fmt.parseFloat(f32, valueString);
                        } else {
                            break :blk try std.fmt.parseInt(zigType, valueString, 10);
                        }
                    }
                };
                @ptrCast(*zigType, @alignCast(@alignOf(zigType), &self.data[offset])).* = value;
            },
        }
    }
};

pub const CsvFileParserAuto = struct {
    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    delim: []const u8,
    metadata: CsvMetadata,
    metadataExt: CsvMetadataExt,
    data: CsvData,

    const Self = @This();

    pub fn init(filePath: []const u8, delim: []const u8, allocator: std.mem.Allocator) !Self
    {
        var self = Self {
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .delim = delim,
            .metadata = undefined,
            .metadataExt = undefined,
            .data = undefined,
        };

        const arenaAllocator = self.arena.allocator();
        var tempArena = std.heap.ArenaAllocator.init(self.allocator);
        defer tempArena.deinit();
        const tempAllocator = tempArena.allocator();

        var parseState = try tempAllocator.create(ParseState);
        self.metadata = try CsvMetadata.init(filePath, self.delim, parseState);
        self.metadataExt = try CsvMetadataExt.init(
            self.metadata.numColumns,
            filePath,
            0,
            self.metadata.fileSize,
            self.delim,
            parseState,
            arenaAllocator,
            arenaAllocator
        );
        self.data = try CsvData.init(
            self.metadataExt,
            filePath,
            0,
            self.metadata.fileSize,
            self.delim,
            parseState,
            arenaAllocator
        );

        return self;
    }

    pub fn deinit(self: *Self) void
    {
        self.arena.deinit();
    }

    pub fn numRows(self: *const Self) usize
    {
        return self.metadataExt.numRows;
    }

    pub fn numColumns(self: *const Self) usize
    {
        return self.metadata.numColumns;
    }
};
