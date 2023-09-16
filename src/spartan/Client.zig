//! Spartan client
//! Maybe: Connecting and opening requests are threadsafe. Individual requests are not.

const std = @import("std");
const testing = std.testing;
const spartan = @import("../spartan.zig");
const mem = std.mem;
const net = std.net;
const Uri = std.Uri;
const Allocator = mem.Allocator;
const assert = std.debug.assert;

const Client = @This();

allocator: Allocator,

/// The pool of connections that can be reused (and currently in use).
connection_pool: ConnectionPool = .{},

proxy: ?SpartanProxy = null,
// Below is code adapted from std.http.Client

pub fn deinit(client: *Client) void {
    client.connection_pool.deinit(client);
    client.* = undefined;
}

pub const RequestError =
    ConnectUnproxiedError ||
    ConnectErrorPartial ||
    std.fmt.ParseIntError ||
    Connection.WriteError ||
    error{ UnsupportedUrlScheme, UriMissingHost };

pub const ConnectUnproxiedError = Allocator.Error || error{
    ConnectionRefused,
    NetworkUnreachable,
    ConnectionTimedOut,
    ConnectionResetByPeer,
    TemporaryNameServerFailure,
    NameServerFailure,
    UnknownHostName,
    HostLacksNetworkAddresses,
    UnexpectedConnectFailure,
};

const ConnectErrorPartial =
    ConnectUnproxiedError ||
    error{ UnsupportedUrlScheme, ConnectionRefused };

pub const ConnectError = ConnectErrorPartial || RequestError;

pub fn connect(
    client: *Client,
    host: []const u8,
    port: u16,
) ConnectError!*ConnectionPool.Node {
    if (client.connection_pool.findConnection(.{
        .host = host,
        .port = port,
    })) |node| {
        return node;
    }

    if (client.proxy) |proxy| {
        const proxy_port: u16 = proxy.port orelse 300;

        const conn = try client.connectUnproxied(proxy.host, proxy_port);
        conn.data.proxied = true;

        return conn;
    } else {
        return client.connectUnproxied(host, port);
    }
}

/// Connect to `host:port` using the specified protocol. This will reuse a connection if one is already open.
/// This function is threadsafe.
pub fn connectUnproxied(
    client: *Client,
    host: []const u8,
    port: u16,
) ConnectUnproxiedError!*ConnectionPool.Node {
    if (client.connection_pool.findConnection(.{
        .host = host,
        .port = port,
    })) |node| {
        return node;
    }

    const conn = try client.allocator.create(ConnectionPool.Node);
    errdefer client.allocator.destroy(conn);
    conn.* = .{ .data = undefined };

    const stream = net.tcpConnectToHost(client.allocator, host, port) catch |err| switch (err) {
        error.ConnectionRefused => return error.ConnectionRefused,
        error.NetworkUnreachable => return error.NetworkUnreachable,
        error.ConnectionTimedOut => return error.ConnectionTimedOut,
        error.ConnectionResetByPeer => return error.ConnectionResetByPeer,
        error.TemporaryNameServerFailure => return error.TemporaryNameServerFailure,
        error.NameServerFailure => return error.NameServerFailure,
        error.UnknownHostName => return error.UnknownHostName,
        error.HostLacksNetworkAddresses => return error.HostLacksNetworkAddresses,
        else => return error.UnexpectedConnectFailure,
    };
    errdefer stream.close();

    conn.data = .{
        .stream = stream,

        .host = try client.allocator.dupe(u8, host),
        .port = port,
    };
    errdefer client.allocator.free(conn.data.host);

    client.connection_pool.addUsed(conn);

    return conn;
}

pub const Response = struct {
    content: Content,

    pub const Content = union(spartan.Status) {
        success: Body,
        redirect: []const u8,
        client_error: []const u8,
        server_error: []const u8,

        pub const Body = struct {
            mime: []const u8,
            body: []const u8,
        };
    };

    pub fn deinit(res: Response, allocator: Allocator) void {
        switch (res.content) {
            .success => |body| {
                allocator.free(body.mime);
                allocator.free(body.body);
            },
            inline else => |s| allocator.free(s),
        }
    }
};

pub const Options = struct {
    /// If set to `false` will make `sendRequest` return status: 3 redirect
    /// responses instead of following the redirects
    handle_redirects: bool = true,
    max_redirects: u32 = 5,
    max_size: usize = std.math.maxInt(usize),

    /// Must be an already acquired connection.
    connection: ?*ConnectionPool.Node = null,
};

pub fn sendRequest(client: *Client, uri: Uri, data: ?[]const u8, options: Options) !Response {
    if (!std.mem.eql(u8, "spartan", uri.scheme)) return error.UnsupportedScheme;

    const port: u16 = uri.port orelse 300;

    const host = uri.host orelse return error.UriMissingHost;

    var proper_data = try client.allocator.dupe(u8, if (data) |d| d else "");
    defer client.allocator.free(proper_data);
    if (uri.query) |query| {
        client.allocator.free(proper_data);
        proper_data = try Uri.unescapeString(client.allocator, query);
    }

    const conn = options.connection orelse try client.connect(host, port);

    var path = if (uri.path.len > 0) uri.path else "/";

    if (!options.handle_redirects) {
        return requestRaw(
            client,
            &conn.data,
            path,
            proper_data,
            options.max_size,
        );
    }

    var path_alloc = std.heap.ArenaAllocator.init(client.allocator);
    defer path_alloc.deinit();

    var redirect_count: u32 = 0;
    while (redirect_count < options.max_redirects) {
        const response = try requestRaw(
            client,
            &conn.data,
            path,
            proper_data,
            options.max_size,
        );
        switch (response.content) {
            .redirect => |redirect| {
                path = try path_alloc.allocator().dupe(u8, redirect);
                redirect_count += 1;
            },
            else => return response,
        }
        response.deinit(client.allocator);
    }
    return error.TooManyRedirects;
}

fn requestRaw(
    client: *Client,
    connection: *Connection,
    path: []const u8,
    data: ?[]const u8,
    max_size: usize,
) !Response {
    var work_buf: [1500]u8 = undefined;

    const data_len = if (data) |d| d.len else 0;

    const request_str = try std.fmt.bufPrint(
        &work_buf,
        "{s} {s} {}\r\n",
        .{ connection.host, path, data_len },
    );

    try connection.writeAll(request_str);
    if (data) |dat| {
        try connection.writeAll(dat);
    }
    var res = Response{ .content = undefined };

    var buff = std.io.fixedBufferStream(&work_buf);
    var inp = buff.writer();

    try connection.reader().streamUntilDelimiter(inp, '\n', null);
    const response_header = buff.getWritten();
    if (response_header.len < 3) {
        return error.InvalidSpartanResponse;
    }

    if (response_header[response_header.len - 1] != '\r') {
        return error.InvalidSpartanResponse;
    }

    if (!std.ascii.isDigit(response_header[0])) {
        return error.InvalidSpartanResponse;
    }

    const info = std.mem.trim(u8, response_header[1..], " \t\r\n");
    res.content = switch (response_header[0]) {
        '2' => success: {
            const mime = try client.allocator.dupe(u8, info);
            errdefer client.allocator.free(mime);
            const body = try connection.reader().readAllAlloc(client.allocator, max_size);

            break :success Response.Content{
                .success = .{
                    .mime = mime,
                    .body = body,
                },
            };
        },
        '3' => redirect: {
            const new_path = try client.allocator.dupe(u8, info);

            break :redirect Response.Content{
                .redirect = new_path,
            };
        },
        '4' => client: {
            const errmsg = try client.allocator.dupe(u8, info);

            break :client Response.Content{
                .client_error = errmsg,
            };
        },
        '5' => server: {
            const errmsg = try client.allocator.dupe(u8, info);

            break :server Response.Content{
                .server_error = errmsg,
            };
        },
        else => return error.InvalidSpartanResponse,
    };

    return res;
}

/// A set of linked lists of connections that can be reused.
pub const ConnectionPool = struct {
    /// The criteria for a connection to be considered a match.
    pub const Criteria = struct {
        host: []const u8,
        port: u16,
    };

    const Queue = std.DoublyLinkedList(Connection);
    pub const Node = Queue.Node;

    mutex: std.Thread.Mutex = .{},
    /// Open connections that are currently in use.
    used: Queue = .{},
    /// Open connections that are not currently in use.
    free: Queue = .{},
    free_len: usize = 0,
    free_size: usize = 32,

    /// Finds and acquires a connection from the connection pool matching the criteria. This function is threadsafe.
    /// If no connection is found, null is returned.
    pub fn findConnection(pool: *ConnectionPool, criteria: Criteria) ?*Node {
        pool.mutex.lock();
        defer pool.mutex.unlock();

        var next = pool.free.last;
        while (next) |node| : (next = node.prev) {
            if (node.data.port != criteria.port) continue;
            if (!mem.eql(u8, node.data.host, criteria.host)) continue;

            pool.acquireUnsafe(node);
            return node;
        }

        return null;
    }

    /// Acquires an existing connection from the connection pool. This function is not threadsafe.
    pub fn acquireUnsafe(pool: *ConnectionPool, node: *Node) void {
        pool.free.remove(node);
        pool.free_len -= 1;

        pool.used.append(node);
    }

    /// Acquires an existing connection from the connection pool. This function is threadsafe.
    pub fn acquire(pool: *ConnectionPool, node: *Node) void {
        pool.mutex.lock();
        defer pool.mutex.unlock();

        return pool.acquireUnsafe(node);
    }

    /// Tries to release a connection back to the connection pool. This function is threadsafe.
    /// If the connection is marked as closing, it will be closed instead.
    pub fn release(pool: *ConnectionPool, client: *Client, node: *Node) void {
        pool.mutex.lock();
        defer pool.mutex.unlock();

        pool.used.remove(node);

        if (node.data.closing) {
            node.data.deinit(client);
            return client.allocator.destroy(node);
        }

        if (pool.free_len >= pool.free_size) {
            const popped = pool.free.popFirst() orelse unreachable;
            pool.free_len -= 1;

            popped.data.deinit(client);
            client.allocator.destroy(popped);
        }

        if (node.data.proxied) {
            pool.free.prepend(node); // proxied connections go to the end of the queue, always try direct connections first
        } else {
            pool.free.append(node);
        }

        pool.free_len += 1;
    }

    /// Adds a newly created node to the pool of used connections. This function is threadsafe.
    pub fn addUsed(pool: *ConnectionPool, node: *Node) void {
        pool.mutex.lock();
        defer pool.mutex.unlock();

        pool.used.append(node);
    }

    pub fn deinit(pool: *ConnectionPool, client: *Client) void {
        pool.mutex.lock();

        var next = pool.free.first;
        while (next) |node| {
            defer client.allocator.destroy(node);
            next = node.next;

            node.data.deinit(client);
        }

        next = pool.used.first;
        while (next) |node| {
            defer client.allocator.destroy(node);
            next = node.next;

            node.data.deinit(client);
        }

        pool.* = undefined;
    }
};

/// An interface to a plain connection.
pub const Connection = struct {
    stream: net.Stream,

    host: []u8,
    port: u16,

    proxied: bool = false,
    closing: bool = false,

    pub fn readAtLeast(conn: *Connection, buffer: []u8, len: usize) ReadError!usize {
        return conn.stream.readAtLeast(buffer, len) catch |err| {
            switch (err) {
                error.ConnectionTimedOut => return error.ConnectionTimedOut,
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

    pub fn writeAll(conn: *Connection, buffer: []const u8) !void {
        return conn.stream.writeAll(buffer) catch |err| switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => return error.ConnectionResetByPeer,
            else => return error.UnexpectedWriteFailure,
        };
    }

    pub fn write(conn: *Connection, buffer: []const u8) !usize {
        return conn.stream.write(buffer) catch |err| switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => return error.ConnectionResetByPeer,
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

    pub fn deinit(conn: *Connection, client: *const Client) void {
        conn.close();
        client.allocator.free(conn.host);
    }
};

pub const SpartanProxy = struct {
    host: []const u8,
    port: ?u16 = null,
};

test {
    _ = testing.refAllDecls(@This());
}
