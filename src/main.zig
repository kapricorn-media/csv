const std = @import("std");

const csv = @import("csv.zig");
const stats = @import("stats.zig");

pub fn main() !void
{
    var gpa = std.heap.GeneralPurposeAllocator(.{}) {};
    defer _ = gpa.deinit();

    // const filePath = "short.csv";
    const filePath = "aella.csv";
    // const filePath = "100MB.csv";
    // const filePath = "800MB.csv";
    // const filePath = "4GB.csv";

    const timeStart = std.time.nanoTimestamp();

    var csvParser = try csv.CsvFileParserAuto.init(filePath, ",", gpa.allocator());
    defer csvParser.deinit();

    const timeEnd = std.time.nanoTimestamp();
    const elapsedNs = @intToFloat(f64, timeEnd) - @intToFloat(f64, timeStart);
    const elapsedS = elapsedNs / 1000.0 / 1000.0 / 1000.0;

    for (csvParser.metadataExt.columnNames) |_, i| {
        std.debug.print("{s}: {}\n", .{
            csvParser.metadataExt.columnNames[i], csvParser.metadataExt.columnTypes[i]
        });
    }

    const numSampleRows = 5;
    if (csvParser.metadataExt.numRows >= numSampleRows) {
        var row: usize = 0;
        while (row < numSampleRows) : (row += 1) {
            for (csvParser.metadataExt.columnTypes) |columnType, col| {
                switch (columnType) {
                    .none, .string => {
                        std.debug.print(",", .{});
                    },
                    inline else => |ct| {
                        const offsetBase = csvParser.metadataExt.columnOffsets[col];
                        const offset = offsetBase + row * csv.getZigTypeSize(ct);
                        const zigType = comptime csv.getZigType(ct);
                        const value = @ptrCast(*zigType, @alignCast(@alignOf(zigType), &csvParser.data.data[offset])).*;
                        std.debug.print("{},", .{value});
                    },
                }
            }
            std.debug.print("\n", .{});
        }
    }

    if (csvParser.getColumnValues("sexw")) |cv| {
        const values = cv.f32;
        // for (values) |v| {
        //     std.debug.print("{d:.3} ", .{v});
        // }
        // std.debug.print("\n", .{});
        std.debug.print("sexw mean: {d:.3}\n", .{stats.mean(f32, values)});
    }
    if (csvParser.getColumnValues("cis")) |cv| {
        const values = cv.i8;
        std.debug.print("cis  mean: {d:.3}\n", .{stats.mean(i8, values)});
    }

    std.debug.print("Parsed {d:.3} MB, {} rows, {} columns, {d:.3} MB data size\n{d:.3} seconds\n", .{
        @intToFloat(f32, csvParser.metadata.fileSize) / 1024 / 1024,
        csvParser.numRows(),
        csvParser.numColumns(),
        @intToFloat(f32, csvParser.data.data.len) / 1024 / 1024,
        elapsedS,
    });
}

test "Zig slice test" {
    const str = [_]u8{'h', 'e', 'l', 'l', 'o'};

    const strSlice = str[0..];
    try std.testing.expectEqual(5, strSlice.len);
    try std.testing.expectEqualSlices(u8, "hello", strSlice);
    const sub1 = strSlice[1..];
    try std.testing.expectEqualSlices(u8, "ello", sub1);
    const sub2 = strSlice[4..];
    try std.testing.expectEqualSlices(u8, "o", sub2);
    const sub3 = strSlice[5..]; // 1 past the end is fine
    try std.testing.expectEqual(0, sub3.len);
    try std.testing.expectEqualSlices(u8, "", sub3);
}
