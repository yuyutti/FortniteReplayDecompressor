using FortniteReplayReader;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Unreal.Core.Models.Enums;

#nullable enable

// Set up dependency injection and logging services
var serviceCollection = new ServiceCollection()
    .AddLogging(loggingBuilder => loggingBuilder
        .AddConsole()
        .SetMinimumLevel(LogLevel.Warning));
var provider = serviceCollection.BuildServiceProvider();
var logger = provider.GetService<ILogger<Program>>();

IEnumerable<string> replayFiles;
var machineReadableOutput = false;
var resolvedInputPath = string.Empty;

static string NormalizeReplayAlias(string inputPath)
{
    if (string.Equals(inputPath, "oldVer-client.replay", StringComparison.OrdinalIgnoreCase))
    {
        return "oldVer-cliant.replay";
    }

    return inputPath;
}

static string? TryResolveReplayPath(string inputPath, IEnumerable<string> searchRoots)
{
    inputPath = NormalizeReplayAlias(inputPath);

    foreach (var root in searchRoots.Where(Directory.Exists))
    {
        var directCandidate = Path.Combine(root, inputPath);
        if (File.Exists(directCandidate))
        {
            return directCandidate;
        }

        var fileName = Path.GetFileName(inputPath);
        var recursiveCandidate = Directory.EnumerateFiles(root, fileName, SearchOption.AllDirectories).FirstOrDefault();
        if (!string.IsNullOrEmpty(recursiveCandidate))
        {
            return recursiveCandidate;
        }
    }

    return null;
}

static bool IsLikelyServerReplay(string inputPath) =>
    Path.GetFileNameWithoutExtension(inputPath)
        .Contains("server", StringComparison.OrdinalIgnoreCase);

if (args.Length > 0)
{
    var inputPath = NormalizeReplayAlias(args[0]);
    var demosFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), @"FortniteGame\Saved\Demos");
    var searchRoots = new[]
    {
        demosFolder,
        Path.Combine(Directory.GetCurrentDirectory(), "Replays"),
        Directory.GetCurrentDirectory()
    };

    if (!Path.IsPathRooted(inputPath))
    {
        var resolvedCandidate = TryResolveReplayPath(inputPath, searchRoots);
        if (!string.IsNullOrEmpty(resolvedCandidate))
        {
            inputPath = resolvedCandidate;
        }
    }

    if (!Path.IsPathRooted(inputPath))
    {
        inputPath = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), inputPath));
    }

    if (File.Exists(inputPath))
    {
        replayFiles = new[] { inputPath };
        resolvedInputPath = inputPath;
        machineReadableOutput = true;
    }
    else if (Directory.Exists(inputPath))
    {
        replayFiles = Directory.EnumerateFiles(inputPath, "*.replay");
        resolvedInputPath = inputPath;
    }
    else
    {
        Console.Error.WriteLine($"Input path not found: {inputPath}");
        return;
    }
}
else
{
    var candidates = new[]
    {
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), @"FortniteGame\Saved\Demos"),
        Path.Combine(Directory.GetCurrentDirectory(), "Replays")
    };

    var replayFilesFolder = candidates.FirstOrDefault(Directory.Exists);
    if (string.IsNullOrEmpty(replayFilesFolder))
    {
        Console.Error.WriteLine("No replay folder found. Pass a replay file path or directory as argument.");
        return;
    }

    replayFiles = Directory.EnumerateFiles(replayFilesFolder, "*.replay");
    resolvedInputPath = replayFilesFolder;
}

var sw = new Stopwatch();
long total = 0;
var jsonOptions = new JsonSerializerOptions
{
    WriteIndented = false,
    NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals
};

const long LargeReplayThresholdBytes = 100L * 1024 * 1024;
var singleReplayParseMode = ParseMode.Normal;
var parseModeOverride = Environment.GetEnvironmentVariable("REPLAY_PARSE_MODE");
if (!string.IsNullOrWhiteSpace(parseModeOverride) &&
    Enum.TryParse<ParseMode>(parseModeOverride, ignoreCase: true, out var overriddenParseMode))
{
    singleReplayParseMode = overriddenParseMode;
}

if (machineReadableOutput && !string.IsNullOrEmpty(resolvedInputPath))
{
    try
    {
        var replayLength = new FileInfo(resolvedInputPath).Length;
        if (string.IsNullOrWhiteSpace(parseModeOverride) && replayLength >= LargeReplayThresholdBytes)
        {
            singleReplayParseMode = IsLikelyServerReplay(resolvedInputPath)
                ? ParseMode.EventsOnly
                : ParseMode.Normal;
        }
    }
    catch
    {
        singleReplayParseMode = ParseMode.Normal;
    }
}

#if DEBUG
var reader = machineReadableOutput
    ? new ReplayReader(null, singleReplayParseMode)
    : new ReplayReader(logger, ParseMode.Normal);
#else
var reader = new ReplayReader(null, machineReadableOutput ? singleReplayParseMode : ParseMode.Minimal);
#endif

static object ProjectPlayerData(FortniteReplayReader.Models.PlayerData playerData)
{
    return new
    {
        playerData.Id,
        playerData.PlayerId,
        playerData.EpicId,
        playerData.PlatformUniqueNetId,
        playerData.BotId,
        playerData.IsBot,
        playerData.PlayerName,
        playerData.PlayerNameCustomOverride,
        playerData.StreamerModeName,
        playerData.Platform,
        playerData.Level,
        playerData.SeasonLevelUIDisplay,
        playerData.InventoryId,
        playerData.PlayerNumber,
        playerData.TeamIndex,
        playerData.IsPartyLeader,
        playerData.IsReplayOwner,
        playerData.IsGameSessionOwner,
        playerData.HasFinishedLoading,
        playerData.HasStartedPlaying,
        playerData.HasThankedBusDriver,
        playerData.IsUsingStreamerMode,
        playerData.IsUsingAnonymousMode,
        playerData.Disconnected,
        playerData.RebootCounter,
        playerData.Placement,
        playerData.Kills,
        playerData.TeamKills,
        playerData.DeathCause,
        playerData.DeathCircumstance,
        playerData.DeathTags,
        playerData.DeathLocation,
        playerData.DeathTime,
        playerData.DeathTimeDouble,
        playerData.Cosmetics,
        playerData.CurrentWeapon,
        LocationCount = playerData.Locations?.Count ?? 0
    };
}

if (machineReadableOutput)
{
    try
    {
        var replay = reader.ReadReplay(resolvedInputPath);

        var output = new
        {
            Eliminations = replay.Eliminations ?? Array.Empty<FortniteReplayReader.Models.Events.PlayerElimination>(),
            Stats = replay.Stats,
            TeamStats = replay.TeamStats,
            GameData = replay.GameData,
            TeamData = replay.TeamData ?? Array.Empty<FortniteReplayReader.Models.TeamData>(),
            PlayerData = (replay.PlayerData ?? Array.Empty<FortniteReplayReader.Models.PlayerData>())
                .Select(ProjectPlayerData)
                .ToArray(),
            KillFeed = replay.KillFeed ?? Array.Empty<FortniteReplayReader.Models.KillFeedEntry>(),
            MapData = replay.MapData,
            Info = replay.Info,
            Header = replay.Header
        };

        Console.Out.Write(JsonSerializer.Serialize(output, jsonOptions));
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex);
        Environment.ExitCode = 1;
    }

    return;
}

foreach (var replayFile in replayFiles)
{
    sw.Restart();
    try
    {
        var replay = reader.ReadReplay(replayFile);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex);
    }
    sw.Stop();
    Console.WriteLine($"---- {replayFile} : done in {sw.ElapsedMilliseconds} milliseconds ----");
    total += sw.ElapsedMilliseconds;
}

Console.WriteLine($"total: {total / 1000} seconds ----");
