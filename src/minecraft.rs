// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! The ports a Minecraft server listens on, for the tests' example specs. blocklyd itself never
//! knows them: the control plane names a workload's ports, and these are what it names for one
//! server. Nothing exports them, so they are written once, here.

/// The game: `server-port` in server.properties (<https://minecraft.wiki/w/Server.properties>).
pub const GAME_PORT: u16 = 25565;
/// RCON: `rcon.port` in server.properties.
pub const RCON_PORT: u16 = 25575;
/// Simple Voice Chat's, a mod's, over UDP: `port` in its config/voicechat/voicechat-server.properties.
pub const VOICE_PORT: u16 = 24454;
