const std = @import("std");

pub fn numberToF64(comptime T: type, value: T) f64
{
    return switch (@typeInfo(T)) {
        .Int => @intToFloat(f64, value),
        .Float => @as(f64, value),
        else => unreachable,
    };
}

pub fn sum(comptime T: type, values: []const T) f64
{
    var s: f64 = 0;
    for (values) |v| {
        s += numberToF64(T, v);
    }
    return s;
}

pub fn mean(comptime T: type, values: []const T) f64
{
    const s = sum(T, values);
    return s / @intToFloat(f64, values.len);
}
