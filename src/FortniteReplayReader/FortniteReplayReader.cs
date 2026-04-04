using FortniteReplayReader.Exceptions;
using FortniteReplayReader.Extensions;
using FortniteReplayReader.Models;
using FortniteReplayReader.Models.Enums;
using FortniteReplayReader.Models.Events;
using FortniteReplayReader.Models.NetFieldExports;
using FortniteReplayReader.Models.NetFieldExports.Weapons;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using Unreal.Core;
using Unreal.Core.Contracts;
using Unreal.Core.Exceptions;
using Unreal.Core.Models;
using Unreal.Core.Models.Enums;
using Unreal.Encryption;
using System.Collections.Generic;

namespace FortniteReplayReader;

public class ReplayReader : Unreal.Core.ReplayReader<FortniteReplay>
{
    private FortniteReplayBuilder Builder;

    public ReplayReader(ILogger logger = null, ParseMode parseMode = ParseMode.Minimal) : base(logger, parseMode)
    {
    }

    public FortniteReplay ReadReplay(string fileName)
    {
        using var stream = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        return ReadReplay(stream);
    }

    public FortniteReplay ReadReplay(Stream stream)
    {
        using var archive = new Unreal.Core.BinaryReader(stream);

        Builder = new FortniteReplayBuilder();
        ReadReplay(archive);

        return Builder.Build(Replay);
    }

    private string _branch;
    private bool _isLikelyNewReplayFormat;
    public int Major { get; set; }
    public int Minor { get; set; }
    public string Branch
    {
        get => _branch;
        set
        {
            var regex = new Regex(@"(?:\+\+Fortnite\+Release\-|Fortnite\+Release\-|Release\-)?(?<major>\d+)\.(?<minor>\d+)");
            var result = regex.Match(value);
            if (result.Success)
            {
                Major = int.Parse(result.Groups["major"]?.Value ?? "0");
                Minor = int.Parse(result.Groups["minor"]?.Value ?? "0");
            }
            _branch = value;
        }
    }

    /// <summary>
    /// Determines if this is a new server replay format (version 28+).
    /// New server replays have different property formats and compression methods.
    /// </summary>
    public bool IsNewServerReplay => Major >= 28 || _isLikelyNewReplayFormat;

    /// <summary>
    /// Determines if this is a new client replay format (version 27+).
    /// </summary>
    public bool IsNewClientReplay => Major >= 27;

    protected override void OnChannelOpened(uint channelIndex, NetworkGUID? actor)
    {
        if (actor != null)
        {
            Builder.AddActorChannel(channelIndex, actor.Value);
        }
    }

    protected override void OnChannelClosed(uint channelIndex, NetworkGUID? actor)
    {
        if (actor != null)
        {
            Builder.RemoveChannel(channelIndex);
        }
    }

    protected override void OnNetDeltaRead(uint channelIndex, NetDeltaUpdate update)
    {
        switch (update.Export)
        {
            case ActiveGameplayModifier modifier:
                Builder.UpdateGameplayModifiers(modifier);
                break;
            //case FortPickup pickup:
            //Builder.CreatePickupEvent(channelIndex, pickup);
            //break;
            //case FortInventory inventory:
            //    Builder.UpdateInventory(channelIndex, inventory);
            //    break;
            case SpawnMachineRepData spawnMachine:
                Builder.UpdateRebootVan(channelIndex, spawnMachine);
                break;
        }
    }

    protected override void OnExportRead(uint channelIndex, INetFieldExportGroup? exportGroup)
    {
        try
        {
            switch (exportGroup)
            {
                case GameState state:
                    Builder.UpdateGameState(state);
                    break;
                case PlaylistInfo playlist:
                    Builder.UpdatePlaylistInfo(playlist);
                    break;
                case FortPlayerState state:
                    Builder.UpdatePlayerState(channelIndex, state);
                    break;
                case PlayerPawn pawn:
                    Builder.UpdatePlayerPawn(channelIndex, pawn);
                    break;
                //case FortPickup pickup:
                //Builder.CreatePickupEvent(channelIndex, pickup);
                //break;
                //case FortInventory inventory:
                //    Builder.UpdateInventory(channelIndex, inventory);
                //    break;
                //case BroadcastExplosion explosion:
                //    Builder.UpdateExplosion(explosion);
                //    break;
                case SafeZoneIndicator safeZone:
                    Builder.UpdateSafeZones(safeZone);
                    break;
                case SupplyDropLlama llama:
                    Builder.UpdateLlama(channelIndex, llama);
                    break;
                case Models.NetFieldExports.SupplyDrop drop:
                    Builder.UpdateSupplyDrop(channelIndex, drop);
                    break;
                case FortPoiManager poimanager:
                    Builder.UpdatePoiManager(poimanager);
                    break;
                //case GameplayCue gameplayCue:
                //    Builder.UpdateGameplayCue(channelIndex, gameplayCue);
                //    break;
                case BaseWeapon weapon:
                    Builder.UpdateWeapon(channelIndex, weapon);
                    break;
                default:
                    if (IsNewServerReplay && exportGroup != null)
                    {
                        _logger?.LogWarning(
                            "Unknown property group for new replay format: {Type} at version {Major}.{Minor} (Enum: {Export})",
                            exportGroup.GetType().Name, Major, Minor, exportGroup.GetType().FullName
                        );
                    }
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex,
                "Failed to parse property group {Type} at version {Major}.{Minor}, Channel {ChannelIndex}",
                exportGroup?.GetType().Name, Major, Minor, channelIndex
            );

            if (!IsNewServerReplay)
                throw; // Throw for older versions to maintain strict parsing
        }
    }

    protected override void OnExternalDataRead(uint channelIndex, IExternalData? externalData)
    {
        // TODO: at the very least, only use PlayerNameData when handle and netfieldgroup match...
        if (externalData != null)
        {
            Builder.UpdatePrivateName(channelIndex, new PlayerNameData(externalData.Archive));
        }
    }

    protected override bool IgnoreGroupOnChannel(uint channelIndex, INetFieldExportGroup exportGroup) => exportGroup switch
    {
        FortPlayerState when _parseMode == ParseMode.Minimal => true,
        GameState when _parseMode == ParseMode.Minimal => true,
        // Cosmetics and movement are noisy; the first useful snapshot is enough for our CLI JSON output.
        PlayerPawn => true,
        BaseWeapon => true,
        ActiveGameplayModifier => true,
        PlaylistInfo => true,
        FortPoiManager => true,
        _ => false
    };

    public override void ReadReplayHeader(FArchive archive)
    {
        base.ReadReplayHeader(archive);
        Branch = Replay.Header.Branch ?? string.Empty;
        _isLikelyNewReplayFormat =
            Replay.Header.EngineNetworkVersion > EngineNetworkVersionHistory.LATEST ||
            Replay.Header.NetworkVersion > NetworkVersionHistory.LATEST ||
            Major >= 28;

        if (_isLikelyNewReplayFormat && Major == 0)
        {
            Major = 28;
            Minor = 0;
        }

        // Log replay version information for debugging
        _logger?.LogInformation(
            "Replay Version: {Major}.{Minor} (Branch: {Branch})",
            Major, Minor, Branch
        );
        _logger?.LogInformation(
            "Network Version: {NetworkVersion}, Engine Version: {EngineVersion}",
            Replay.Header.NetworkVersion, Replay.Header.EngineNetworkVersion
        );
        _logger?.LogInformation(
            "Compressed: {IsCompressed}, Encrypted: {IsEncrypted}, Format: {Format}",
            Replay.Info.IsCompressed, Replay.Info.IsEncrypted,
            IsNewServerReplay ? "New (Server)" : IsNewClientReplay ? "New (Client)" : "Legacy"
        );
    }

    /// <summary>
    /// Override frame reading to support new replay formats (v27+).
    /// New formats have different frame data structures that may not be fully compatible
    /// with the legacy parsing logic.
    /// </summary>
    public override void ReadDemoFrameIntoPlaybackPackets(FArchive archive)
    {
        try
        {
            var shouldUseNewFormatFrameReader =
                IsNewServerReplay ||
                archive.EngineNetworkVersion > EngineNetworkVersionHistory.LATEST ||
                archive.NetworkVersion > NetworkVersionHistory.LATEST;

            // For new server replays, use alternative frame reading logic
            // to handle potential format differences
            if (shouldUseNewFormatFrameReader)
            {
                _logger?.LogDebug(
                    "Reading frame with resilient packet parsing for new server replay format (v{Major}.{Minor})",
                    Major,
                    Minor
                );
                base.ReadDemoFrameIntoPlaybackPackets(archive);
            }
            else
            {
                // Legacy format uses the standard reading method
                base.ReadDemoFrameIntoPlaybackPackets(archive);
            }
        }
        catch (ArgumentOutOfRangeException ex)
        {
            _logger?.LogWarning(ex, 
                "Buffer overflow while reading demo frame at v{Major}.{Minor}. " +
                "This may indicate format incompatibility. Frame will be skipped.", 
                Major, Minor);
            if (archive is Unreal.Core.BinaryReader binaryArchive)
            {
                archive.Seek(binaryArchive.Bytes.Length);
            }
        }
    }

    /// <summary>
    /// Read demo frame for new server replay format (v28+).
    /// This handles the potentially different frame structure in newer replays.
    /// </summary>
    private void ReadDemoFrameNewFormat(FArchive archive)
    {
        base.ReadDemoFrameIntoPlaybackPackets(archive);
    }

    /// <summary>
    /// see https://github.com/EpicGames/UnrealEngine/blob/70bc980c6361d9a7d23f6d23ffe322a2d6ef16fb/Engine/Source/Runtime/NetworkReplayStreaming/LocalFileNetworkReplayStreaming/Private/LocalFileNetworkReplayStreaming.cpp#L363
    /// </summary>
    /// <param name="archive"></param>
    /// <returns></returns>
    public override void ReadEvent(FArchive archive)
    {
        var info = new EventInfo
        {
            Id = archive.ReadFString(),
            Group = archive.ReadFString(),
            Metadata = archive.ReadFString(),
            StartTime = archive.ReadUInt32(),
            EndTime = archive.ReadUInt32(),
            SizeInBytes = archive.ReadInt32()
        };

        _logger?.LogDebug("Encountered event {group} ({metadata}) at {startTime} of size {sizeInBytes}", info.Group, info.Metadata, info.StartTime, info.SizeInBytes);

        using var decryptedArchive = DecryptBuffer(archive, info.SizeInBytes);

        // Every event seems to start with some unknown int
        if (info.Group == ReplayEventTypes.PLAYER_ELIMINATION)
        {
            var elimination = ParseElimination(decryptedArchive, info);
            Replay.Eliminations.Add(elimination);
            return;
        }

        else if (info.Metadata == ReplayEventTypes.MATCH_STATS)
        {
            Replay.Stats = ParseMatchStats(decryptedArchive, info);
            return;
        }

        else if (info.Metadata == ReplayEventTypes.TEAM_STATS)
        {
            Replay.TeamStats = ParseTeamStats(decryptedArchive, info);
            return;
        }

        else if (info.Metadata == ReplayEventTypes.ENCRYPTION_KEY)
        {
            ParseEncryptionKeyEvent(decryptedArchive, info);
            return;
        }

        _logger?.LogDebug("Unknown event {group} ({metadata}) of size {sizeInBytes}", info.Group, info.Metadata, info.SizeInBytes);
        if (IsDebugMode)
        {
            throw new UnknownEventException($"Unknown event {info.Group} ({info.Metadata}) of size {info.SizeInBytes}");
        }
    }

    public virtual EncryptionKey ParseEncryptionKeyEvent(FArchive archive, EventInfo info) => new()
    {
        Info = info,
        Key = archive.ReadBytesToString(32)
    };

    public virtual TeamStats ParseTeamStats(FArchive archive, EventInfo info) => new()
    {
        Info = info,
        Unknown = archive.ReadUInt32(),
        Position = archive.ReadUInt32(),
        TotalPlayers = archive.ReadUInt32()
    };

    public virtual Stats ParseMatchStats(FArchive archive, EventInfo info) => new()
    {
        Info = info,
        Unknown = archive.ReadUInt32(),
        Accuracy = archive.ReadSingle(),
        Assists = archive.ReadUInt32(),
        Eliminations = archive.ReadUInt32(),
        WeaponDamage = archive.ReadUInt32(),
        OtherDamage = archive.ReadUInt32(),
        Revives = archive.ReadUInt32(),
        DamageTaken = archive.ReadUInt32(),
        DamageToStructures = archive.ReadUInt32(),
        MaterialsGathered = archive.ReadUInt32(),
        MaterialsUsed = archive.ReadUInt32(),
        TotalTraveled = archive.ReadUInt32()
    };

    public virtual PlayerElimination ParseElimination(FArchive archive, EventInfo info)
    {
        if (archive is Unreal.Core.BinaryReader binaryArchive)
        {
            var bytes = binaryArchive.Bytes.ToArray();
            if (TryParseEliminationStructured(bytes, info, out var parsedElimination) &&
                !IsSuspiciousElimination(parsedElimination))
            {
                return parsedElimination;
            }

            if (TryParseEliminationFromTail(bytes, info, out var recoveredElimination))
            {
                return recoveredElimination;
            }
        }

        try
        {
            return ParseEliminationStructured(archive, info);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error while parsing PlayerElimination at timestamp {}", info?.StartTime);
            throw new PlayerEliminationException($"Error while parsing PlayerElimination at timestamp {info?.StartTime}", ex);
        }
    }

    private PlayerElimination ParseEliminationStructured(FArchive archive, EventInfo info)
    {
        var elim = new PlayerElimination
        {
            Info = info,
        };

        var version = archive.ReadInt32();

        if (version >= 3)
        {
            archive.SkipBytes(1);

            if (version >= 6)
            {
                elim.EliminatedInfo.Rotation = archive.ReadFQuat();
                elim.EliminatedInfo.Location = archive.ReadFVector();
                elim.EliminatedInfo.Scale = archive.ReadFVector();
            }

            elim.EliminatorInfo.Rotation = archive.ReadFQuat();
            elim.EliminatorInfo.Location = archive.ReadFVector();
            elim.EliminatorInfo.Scale = archive.ReadFVector();
        }
        else
        {
            if (Major <= 4 && Minor < 2)
            {
                archive.SkipBytes(8);
            }
            else if (Major == 4 && Minor <= 2)
            {
                archive.SkipBytes(36);
            }
        }

        if ((int) archive.EngineNetworkVersion >= 34)
        {
            archive.SkipBytes(80);
        }

        ParsePlayer(archive, elim.EliminatedInfo, version);
        ParsePlayer(archive, elim.EliminatorInfo, version);

        elim.GunType = archive.ReadByte();
        elim.Knocked = archive.ReadUInt32AsBoolean();
        elim.Time = info.StartTime.MillisecondsToTimeStamp();
        return elim;
    }

    private bool TryParseEliminationStructured(byte[] bytes, EventInfo info, out PlayerElimination elimination)
    {
        try
        {
            using var archive = new Unreal.Core.BinaryReader(bytes.AsMemory())
            {
                EngineNetworkVersion = Replay.Header.EngineNetworkVersion,
                NetworkVersion = Replay.Header.NetworkVersion,
                ReplayHeaderFlags = Replay.Header.Flags,
                ReplayVersion = Replay.Info.FileVersion
            };

            elimination = ParseEliminationStructured(archive, info);
            return true;
        }
        catch
        {
            elimination = null;
            return false;
        }
    }

    private static bool IsSuspiciousElimination(PlayerElimination elimination) =>
        string.IsNullOrWhiteSpace(elimination?.Eliminated) ||
        string.IsNullOrWhiteSpace(elimination?.Eliminator);

    private bool TryParseEliminationFromTail(byte[] bytes, EventInfo info, out PlayerElimination elimination)
    {
        elimination = null;

        if (bytes.Length < 5)
        {
            return false;
        }

        var payloadEnd = bytes.Length - 5;
        var candidates = new List<(int Offset, PlayerEliminationInfo Eliminated, PlayerEliminationInfo Eliminator)>();

        for (var firstOffset = 0; firstOffset < payloadEnd; firstOffset++)
        {
            if (!TryParsePlayerDescriptor(bytes, firstOffset, payloadEnd, out var eliminatedInfo, out var eliminatedLength))
            {
                continue;
            }

            var secondOffset = firstOffset + eliminatedLength;
            if (!TryParsePlayerDescriptor(bytes, secondOffset, payloadEnd, out var eliminatorInfo, out var eliminatorLength))
            {
                continue;
            }

            if (secondOffset + eliminatorLength != payloadEnd)
            {
                continue;
            }

            candidates.Add((firstOffset, eliminatedInfo, eliminatorInfo));
        }

        if (candidates.Count == 0)
        {
            return false;
        }

        var best = candidates.OrderByDescending(candidate => candidate.Offset).First();

        elimination = new PlayerElimination
        {
            Info = info,
            EliminatedInfo = best.Eliminated,
            EliminatorInfo = best.Eliminator,
            GunType = bytes[^5],
            Knocked = BitConverter.ToUInt32(bytes, bytes.Length - 4) >= 1,
            Time = info.StartTime.MillisecondsToTimeStamp()
        };

        return !IsSuspiciousElimination(elimination);
    }

    private static bool TryParsePlayerDescriptor(
        byte[] bytes,
        int offset,
        int payloadEnd,
        out PlayerEliminationInfo info,
        out int consumed)
    {
        info = null;
        consumed = 0;

        if (offset >= payloadEnd)
        {
            return false;
        }

        var playerType = (PlayerTypes) bytes[offset];
        switch (playerType)
        {
            case PlayerTypes.BOT:
                info = new PlayerEliminationInfo
                {
                    PlayerType = playerType,
                    Id = "Bot",
                    Rotation = new FQuat(),
                    Location = new FVector(0, 0, 0),
                    Scale = new FVector(0, 0, 0)
                };
                consumed = 1;
                return true;

            case PlayerTypes.PLAYER:
                if (offset + 2 > payloadEnd)
                {
                    return false;
                }

                var guidSize = bytes[offset + 1];
                if (guidSize == 0 || offset + 2 + guidSize > payloadEnd)
                {
                    return false;
                }

                info = new PlayerEliminationInfo
                {
                    PlayerType = playerType,
                    Id = Convert.ToHexString(bytes.AsSpan(offset + 2, guidSize)),
                    Rotation = new FQuat(),
                    Location = new FVector(0, 0, 0),
                    Scale = new FVector(0, 0, 0)
                };
                consumed = 2 + guidSize;
                return true;

            case PlayerTypes.NAMED_BOT:
                if (offset + 5 > payloadEnd)
                {
                    return false;
                }

                var stringLength = BitConverter.ToInt32(bytes, offset + 1);
                if (stringLength == 0)
                {
                    info = new PlayerEliminationInfo
                    {
                        PlayerType = playerType,
                        Id = string.Empty,
                        Rotation = new FQuat(),
                        Location = new FVector(0, 0, 0),
                        Scale = new FVector(0, 0, 0)
                    };
                    consumed = 5;
                    return true;
                }

                var isUnicode = stringLength < 0;
                var byteLength = isUnicode ? -2 * stringLength : stringLength;
                if (byteLength < 0 || offset + 5 + byteLength > payloadEnd)
                {
                    return false;
                }

                var encoding = isUnicode ? System.Text.Encoding.Unicode : System.Text.Encoding.Default;
                var id = encoding.GetString(bytes, offset + 5, byteLength).Trim(' ', '\0');

                info = new PlayerEliminationInfo
                {
                    PlayerType = playerType,
                    Id = id,
                    Rotation = new FQuat(),
                    Location = new FVector(0, 0, 0),
                    Scale = new FVector(0, 0, 0)
                };
                consumed = 5 + byteLength;
                return !string.IsNullOrWhiteSpace(id);
        }

        return false;
    }

    public virtual void ParsePlayer(FArchive archive, PlayerEliminationInfo info, int version)
    {
        if (version < 6)
        {
            info.Id = archive.ReadFString();
            return;
        }

        info.PlayerType = archive.ReadByteAsEnum<PlayerTypes>();
        info.Id = info.PlayerType switch
        {
            PlayerTypes.BOT => "Bot",
            PlayerTypes.NAMED_BOT => archive.ReadFString(),
            PlayerTypes.PLAYER => archive.ReadGUID(archive.ReadByte()),
            _ => ""
        };
    }

    protected override FArchive DecryptBuffer(FArchive archive, int size)
    {
        if (!Replay.Info.IsEncrypted)
        {
            return new Unreal.Core.BinaryReader(archive.ReadBytes(size))
            {
                EngineNetworkVersion = Replay.Header.EngineNetworkVersion,
                NetworkVersion = Replay.Header.NetworkVersion,
                ReplayHeaderFlags = Replay.Header.Flags,
                ReplayVersion = Replay.Info.FileVersion
            };
        }

        var key = Replay.Info.EncryptionKey;
        var encryptedBytes = archive.ReadBytes(size);

        using var aesCryptoServiceProvider = new AesCryptoServiceProvider
        {
            KeySize = key.Length * 8,
            Key = key.ToArray(),
            Mode = CipherMode.ECB,
            Padding = PaddingMode.PKCS7
        };

        using var cryptoTransform = aesCryptoServiceProvider.CreateDecryptor();
        var decryptedArray = cryptoTransform.TransformFinalBlock(encryptedBytes.ToArray(), 0, encryptedBytes.Length);

        return new Unreal.Core.BinaryReader(decryptedArray.AsMemory())
        {
            EngineNetworkVersion = archive.EngineNetworkVersion,
            NetworkVersion = archive.NetworkVersion,
            ReplayHeaderFlags = archive.ReplayHeaderFlags,
            ReplayVersion = archive.ReplayVersion
        };
    }

    protected override FArchive Decompress(FArchive archive)
    {
        if (!Replay.Info.IsCompressed)
        {
            return archive;
        }

        // Use version-specific decompression for new server replays
        if (IsNewServerReplay)
        {
            return DecompressNewFormat(archive);
        }

        return DecompressLegacyFormat(archive);
    }

    /// <summary>
    /// Legacy decompression format for older Fortnite replays (before v28).
    /// </summary>
    private FArchive DecompressLegacyFormat(FArchive archive)
    {
        var decompressedSize = archive.ReadInt32();
        var compressedSize = archive.ReadInt32();
        var compressedBuffer = archive.ReadBytes(compressedSize);

        try
        {
            _logger?.LogDebug("Decompressing (Legacy) archive from {compressedSize} to {decompressedSize}.", compressedSize, decompressedSize);
            var output = Oodle.DecompressReplayData(compressedBuffer, decompressedSize);

            return new Unreal.Core.BinaryReader(output)
            {
                EngineNetworkVersion = archive.EngineNetworkVersion,
                NetworkVersion = archive.NetworkVersion,
                ReplayHeaderFlags = archive.ReplayHeaderFlags,
                ReplayVersion = archive.ReplayVersion
            };
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to decompress legacy format. Compressed size: {Size}", compressedSize);
            throw;
        }
    }

    /// <summary>
    /// New decompression format for newer Fortnite replays (v28+).
    /// May have different header structure or compression algorithm version.
    /// </summary>
    private FArchive DecompressNewFormat(FArchive archive)
    {
        try
        {
            var decompressedSize = archive.ReadInt32();
            var compressedSize = archive.ReadInt32();
            var compressedBuffer = archive.ReadBytes(compressedSize);

            _logger?.LogDebug("Decompressing (New Format) archive from {compressedSize} to {decompressedSize}.", compressedSize, decompressedSize);

            // Attempt decompression with new format
            var output = Oodle.DecompressReplayData(compressedBuffer, decompressedSize);

            return new Unreal.Core.BinaryReader(output)
            {
                EngineNetworkVersion = archive.EngineNetworkVersion,
                NetworkVersion = archive.NetworkVersion,
                ReplayHeaderFlags = archive.ReplayHeaderFlags,
                ReplayVersion = archive.ReplayVersion
            };
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to decompress new format. Consider updating decompression logic for version {Major}.{Minor}", Major, Minor);
            throw;
        }
    }
}
