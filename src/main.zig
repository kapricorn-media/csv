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

    // for (parserAuto.columnData.items) |c| {
    //     std.debug.print("{s}: {}\n", .{c.name, c.type});
    // }
    // if (parserAuto.rows.items.len >= 10) {
    //     for (parserAuto.rows.items[0..10]) |row| {
    //         std.debug.print("{any}\n\n", .{row});
    //     }
    //     std.debug.print("{any}\n\n", .{parserAuto.rows.items[parserAuto.rows.items.len - 1]});
    // }

    std.debug.print("Parsed {} MB, {} rows, {} columns\n{d:.3} seconds\n", .{
        parserAuto.csvMetadata.fileSize / 1024 / 1024,
        0,
        parserAuto.csvMetadata.numColumns,
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
