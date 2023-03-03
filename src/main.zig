const std = @import("std");

const csv = @import("csv.zig");

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

    var parserAuto = csv.CsvFileParserAuto.init(",", gpa.allocator());
    defer parserAuto.deinit();
    try parserAuto.parse(filePath);

    const timeEnd = std.time.nanoTimestamp();
    const elapsedNs = @intToFloat(f64, timeEnd) - @intToFloat(f64, timeStart);
    const elapsedS = elapsedNs / 1000.0 / 1000.0 / 1000.0;

    for (parserAuto.csvMetadataExt.columnNames) |_, i| {
        std.debug.print("{s}: {}\n", .{
            parserAuto.csvMetadataExt.columnNames[i], parserAuto.csvMetadataExt.columnTypes[i]
        });
    }
    std.debug.print("Parsed {d:.3} MB, {} rows, {} columns, {d:.3} MB data size\n{d:.3} seconds\n", .{
        @intToFloat(f32, parserAuto.csvMetadata.fileSize) / 1024 / 1024,
        parserAuto.csvMetadataExt.numRows,
        parserAuto.csvMetadata.numColumns,
        @intToFloat(f32, parserAuto.csvData.data.len) / 1024 / 1024,
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
