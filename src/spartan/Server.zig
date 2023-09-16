//! Spartan server

const std = @import("std");
const testing = std.testing;
const spartan = @import("../spartan.zig");
const mem = std.mem;
const net = std.net;
const Uri = std.Uri;
const Allocator = mem.Allocator;
const assert = std.debug.assert;

const Server = @This();

allocator: Allocator,

socket: net.StreamServer,

pub fn init(allocator: Allocator, options: net.StreamServer.Options) Server {
    return .{
        .allocator = allocator,
        .socket = net.StreamServer.init(options),
    };
}

pub fn deinit(server: *Server) void {
    server.socket.deinit();
}

pub const ListenError =
    std.os.SocketError || std.os.BindError ||
    std.os.ListenError || std.os.SetSockOptError || std.os.GetSockNameError;

/// Start the Spartan server listening on the given address.
pub fn listen(server: *Server, address: net.Address) !void {
    try server.socket.listen(address);
}

pub const AcceptError = net.StreamServer.AcceptError || Allocator.Error;

pub const Connection = struct {
    pub const buffer_size = std.crypto.tls.max_ciphertext_record_len;
    pub const Protocol = enum { plain };

    stream: net.Stream,
    protocol: Protocol,

    closing: bool = true,

    read_buf: [buffer_size]u8 = undefined,
    read_start: u16 = 0,
    read_end: u16 = 0,

    pub fn readAtLeast(conn: *Connection, buffer: []u8, len: usize) ReadError!usize {
        return conn.stream.readAtLeast(buffer, len) catch |err| {
            switch (err) {
                error.ConnectionResetByPeer,
                error.BrokenPipe,
                => return error.ConnectionResetByPeer,
                else => return error.UnexpectedReadFailure,
            }
        };
    }

    pub fn read(conn: *Connection, buffer: []u8) ReadError!usize {
        return conn.readAtLeast(buffer, 1);
    }

    pub const ReadError = error{
        ConnectionTimedOut,
        ConnectionResetByPeer,
        UnexpectedReadFailure,
        EndOfStream,
    };

    pub const Reader = std.io.Reader(*Connection, ReadError, read);

    pub fn reader(conn: *Connection) Reader {
        return Reader{ .context = conn };
    }

    pub fn writeAll(conn: *Connection, buffer: []const u8) WriteError!void {
        return conn.stream.writeAll(buffer) catch |err| switch (err) {
            error.BrokenPipe,
            error.ConnectionResetByPeer,
            => return error.ConnectionResetByPeer,
            else => return error.UnexpectedWriteFailure,
        };
    }

    pub fn write(conn: *Connection, buffer: []const u8) WriteError!usize {
        return conn.stream.write(buffer) catch |err| switch (err) {
            error.BrokenPipe,
            error.ConnectionResetByPeer,
            => return error.ConnectionResetByPeer,
            else => return error.UnexpectedWriteFailure,
        };
    }

    pub const WriteError = error{
        ConnectionResetByPeer,
        UnexpectedWriteFailure,
    };

    pub const Writer = std.io.Writer(*Connection, WriteError, write);

    pub fn writer(conn: *Connection) Writer {
        return Writer{ .context = conn };
    }

    pub fn close(conn: *Connection) void {
        conn.stream.close();
    }
};
