const std = @import("std");

const zigimg = @import("zigimg");

fn rasterizeRect(
    imgWidth: usize, imgHeight: usize,
    pixels: []zigimg.color.Rgba32,
    left: usize, top: usize,
    width: usize, height: usize,
    color: zigimg.color.Rgba32) void
{
    std.debug.assert(left + width <= imgWidth);
    std.debug.assert(top + height <= imgHeight);

    var y: usize = 0;
    while (y < height) : (y += 1) {
        var x: usize = 0;
        while (x < width) : (x += 1) {
            const ind = (top + y) * imgWidth + x + left;
            pixels[ind] = color;
        }
    }
}

pub fn plot(allocator: std.mem.Allocator, filePath: []const u8) !void
{
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arenaAllocator = arena.allocator();

    const width = 1024;
    const height = 1024;
    var image = try zigimg.Image.create(arenaAllocator, width, height, .rgba32);

    var pixels = image.pixels.rgba32;

    // paint a red square
    rasterizeRect(width, height, pixels, 100, 100, 100, 100, .{.r = 255, .g = 0, .b = 0, .a = 255});

    try image.writeToFilePath(filePath, .{ .png = .{}});
}
