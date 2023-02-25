const std = @import("std");

pub fn CsvFileParser(comptime RowType: type) type
{
    const T = struct {
        allocator: std.mem.Allocator,
        delim: []const u8,
        header: bool,
        // buf: std.ArrayList(u8),
        rows: []RowType,

        const Self = @This();

        pub fn init(delim: []const u8, allocator: std.mem.Allocator) Self
        {
            // const bufInitialCapacity = 16 * 1024;
            return Self {
                .allocator = allocator,
                .delim = delim,
                .header = true,
                // .buf = std.ArrayList(u8).initCapacity(allocator, bufInitialCapacity),
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
            const file = try cwd.openFile(filePath, .{});

            var buf = try self.allocator.alloc(u8, 16 * 1024);
            var totalBytes: usize = 0;
            var rows: usize = 0;
            while (true) {
                const numBytes = try file.read(buf);
                if (numBytes == 0) {
                    break;
                }
                totalBytes += numBytes;
                rows += std.mem.count(u8, buf, "\n");
            }

            std.debug.print("Read {} MB file, {} rows\n", .{totalBytes / 1024 / 1024, rows});

            if (rows > 0) {
                self.rows = try self.allocator.alloc(RowType, rows);
            }

            _ = tempAllocator;
            // var lineBuf = std.ArrayList(u8).init(tempAllocator);
            // try file.seekTo(0);
            // while (true) {
            //     const numBytes = try file.read(buf);
            //     if (numBytes == 0) {
            //         break;
            //     }

            //     const bytes = buf[0..numBytes];
            //     var itLines = std.mem.split(u8, bytes, "\n");
            //     while (itLines.next()) |l| {
            //         lineBuf.clearRetainingCapacity();
            //         try lineBuf.appendSlice(l);
            //     }
            // }

            // var bytes = csvBytes;
            // if (self.header) {
            //     if (std.mem.indexOf(u8, csvBytes, "\n")) |newlineIndex| {
            //         bytes = csvBytes[newlineIndex..];
            //         self.header = false;
            //     }
            // }

            // if (!self.header) {
            //     var itNewline = std.mem.split(u8, bytes, "\n");
            //     while (itNewline.next()) |lineBytes| {
            //         var remaining = lineBytes;
            //         while (true) {
            //             if (std.mem.indexOf(u8, remaining, self.delim)) |i| {
            //                 const value = remaining[0..i];
            //                 _ = value;

            //                 if (i + 1 >= remaining.len) {
            //                     break;
            //                 }
            //                 remaining = remaining[i+1..];
            //             } else {
            //                 if (remaining.len > self.buf.len) {
            //                     return error.BufTooSmall;
            //                 }
            //                 std.mem.copy(u8, &self.buf, remaining);
            //                 break;
            //             }
            //         }
            //     }
            // }
        }
    };
    return T;
}

const Row = struct {
    time_ref: u32,
    account: []const u8,
    code: []const u8,
    country_code: []const u8,
    product_type: []const u8,
    value: f32,
    status: []const u8,
};

pub fn main() !void
{
    var gpa = std.heap.GeneralPurposeAllocator(.{}) {};
    defer _ = gpa.deinit();

    var arenaAllocator = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arenaAllocator.deinit();

    const filePath = "megabytes.csv";
    // const filePath = "gigabytes.csv";

    var parser = CsvFileParser(Row).init("\n", arenaAllocator.allocator());
    try parser.parse(filePath);

    // std.debug.print("Read {} MB\n", .{totalBytes / 1024 / 1024});
}
