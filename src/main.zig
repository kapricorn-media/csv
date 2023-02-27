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


// selfage
// biomale
// gendermale
// cis
// gendered
// Economicliberal
// Socialliberal
// religion
// poly
// How many people are you currently in a romantic relationship with? (m71yew0)
// partnercount
// surveycount
// selfsexuallyopen
// othersexuallyopen
// length
// married
// childcount
// Have you or your partner ever cheated on each other? (hhf9b8h)
// sexrate
// pornrate
// fightrate
// codependent5
// openness1
// codependent2
// codependent6
// """I don't really worry about other attractive people gaining too much of my partner's affection"" (61m55wv)"
// jealousy4
// jealousy2
// friendship2
// friendship3
// friendship1
// How long did you spend in a romantic relationship with your partner before getting married? (anthw9o)
// premarriagetime
// codependent1
// sex4
// friendship5
// fight4
// """If we broke up
//  I think I could date a higher quality person than they could"" (vh27ywp)"
// mismatch1
// healthy6
// mismatch2
// """My partner and I are sexually compatible"" (9nxbebp)"
// sex3
// How long had you been in a romantic relationship with your partner when you had your first child? (qxwjbzq)
// jealousy1
// healthy2
// healthy4
// sex6
// sex2
// jealousy5
// fight6
// healthy5
// codependent4
// fight2
// jealousy3
// openness5
// healthy3
// codependent3
// friendship4
// fight1
// jealousy6
// fight5
// sex1
// sex5
// openness6
// openness2
// friendship6
// healthy1
// mismatch3
// fight3
// honesty
// openness4
// openness3
// mismatch4
// mismatch5
// mismatch6
// fightw
// sexw
// codependentw
// jealousyw
// friendshipw
// healthyw
// opennessw
// mismatchw
// Was this your first time taking the survey? (yg1agly)
// income
// "Roughly speaking
//  what is your yearly income in USD? (8a5otpd)"
// codependent4b
// jealousy5b
// Which category fits you best? (4790ydl)
// How many people total have you had sex with in your life? (dgmye0z)
// sexpartnercount
// firsttime
// fight
// openness
// friendship
// jealousy
// codependent
// healthy
// sex
// mismatch
// ageadj
// lengthadj
// percent
// healthadj

// const Row = struct {
//     time_ref: u32,
//     account: []const u8,
//     code: []const u8,
//     country_code: []const u8,
//     product_type: []const u8,
//     value: f32,
//     status: []const u8,
// };

// aella.csv
const Row = struct {
    _1: u32, // some index thing at the start
    selfage: i8,
    biomale: i8,
    gendermale: i8,
    cis: i8,
    gendered: i8,
    Economicliberal: i8,
    Socialliberal: i8,
    religion: i8,
    poly: i8,
    m71yew0: []const u8,
    partnercount: i8,
    surveycount: i8,
    selfsexuallyopen: i8,
    othersexuallyopen: i8,
    length: i16,
    married: i8,
    childcount: i8,
    hhf9b8h: []const u8,
    sexrate: []const u8,
    pornrate: i16,
    fightrate: i16,
    codependent5: i8,
    openness1: i8,
    codependent2: i8,
    codependent6: i8,
    _61m55wv: []const u8,
    jealousy4: i8,
    jealousy2: i8,
    friendship2: i8,
    friendship3: i8,
    friendship1: i8,
    anthw9o: []const u8,
    premarriagetime: []const u8,
    codependent1: i16,
    sex4: i8,
    friendship5: i8,
    fight4: i8,
    vh27ywp: []const u8,
    mismatch1: i8,
    healthy6: i8,
    mismatch2: i8,
    _9nxbebp: []const u8,
    sex3: i8,
    qxwjbzq: []const u8,
    jealousy1: []const u8,
    healthy2: i8,
    healthy4: i8,
    sex6: i8,
    sex2: i8,
    jealousy5: i8,
    fight6: i8,
    healthy5: i8,
    codependent4: i8,
    fight2: i8,
    jealousy3: i8,
    openness5: i8,
    healthy3: i8,
    codependent3: i8,
    friendship4: i8,
    fight1: i8,
    jealousy6: i8,
    fight5: i8,
    sex1: i8,
    sex5: i8,
    openness6: i8,
    openness2: i8,
    friendship6: i8,
    healthy1: i8,
    mismatch3: i8,
    fight3: i8,
    honesty: i8,
    openness4: i8,
    openness3: i8,
    mismatch4: i8,
    mismatch5: i8,
    mismatch6: i8,
    fightw: f32,
    sexw: f32,
    codependentw: f32,
    jealousyw: f32,
    friendshipw: f32,
    healthyw: f32,
    opennessw: f32,
    mismatchw: f32,
    yg1agly: []const u8,
    income: []const u8,
    _8a5otpd: []const u8,
    codependent4b: []const u8,
    jealousy5b: i8,
    _4790ydl: []const u8,
    dgmye0z: []const u8,
    sexpartnercount: []const u8,
    firsttime: i16,
    fight: i8,
    openness: i8,
    friendship: i8,
    jealousy: i8,
    codependent: i8,
    healthy: i8,
    sex: i8,
    mismatch: i8,
    ageadj: i8,
    lengthadj: f32,
    percent: f32,
    healthadj: f32,
    _2: ?void,
};

pub fn main() !void
{
    var gpa = std.heap.GeneralPurposeAllocator(.{}) {};
    defer _ = gpa.deinit();

    var arenaAllocator = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arenaAllocator.deinit();

    const filePath = "aella.csv";
    // const filePath = "megabytes.csv";
    // const filePath = "gigabytes.csv";

    var parser = CsvFileParser(Row).init(",", arenaAllocator.allocator());
    try parser.parse(filePath);

    if (parser.rows.len > 0) {
        std.debug.print("{}\n", .{parser.rows[0]});
    }
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
