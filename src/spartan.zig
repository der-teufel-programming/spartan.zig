const std = @import("std");

pub const Status = enum(u8) {
    success = 2,
    redirect = 3,
    client_error = 4,
    server_error = 5,

    pub fn phrase(status: Status) []const u8 {
        return switch (status) {
            .success => "SUCCESS",
            .redirect => "REDIRECT",
            .client_error => "CLIENT ERROR",
            .server_error => "SERVER ERROR",
        };
    }
};

pub const Client = @import("spartan/Client.zig");
pub const Server = @import("spartan/Server.zig");

test {
    _ = Client;
    _ = Server;
}
