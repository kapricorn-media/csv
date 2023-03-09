const std = @import("std");

const zigimg = @import("zigimg");

pub fn plot() void
{
    std.log.info("{}", .{zigimg.png.PNG.format()});
}
