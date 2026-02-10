import asyncio
import base64
import logging
import os
from pathlib import Path

import discord
import yaml

from recording.ffmpeg_tools import concat_wavs_to_opus_ogg, mix_opus_ogg
from recording.session import SessionContext, init_session, safe_filename
from src.sinks.whisper_sink import WhisperSink

DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
TRANSCRIPTION_METHOD = os.getenv("TRANSCRIPTION_METHOD")
PLAYER_MAP_FILE_PATH = os.getenv("PLAYER_MAP_FILE_PATH")
CHUNK_SECONDS = int(os.getenv("CHUNK_SECONDS", "30"))
TRANSCRIPTION_LANGUAGE = os.getenv("TRANSCRIPTION_LANGUAGE", "auto")

logger = logging.getLogger(__name__)


class VoloBot(discord.Bot):
    def __init__(self, loop):

        super().__init__(command_prefix="!", loop=loop,
                         activity=discord.CustomActivity(name='Transcribing Audio to Text'))
        self.guild_to_helper = {}
        self.guild_is_recording = {}
        self.guild_whisper_sinks = {}
        self.guild_whisper_message_tasks = {}
        self.guild_transcription_languages: dict[int, str] = {}
        self.active_sessions: dict[int, SessionContext] = {}
        self.player_map = {}
        self._is_ready = False
        self.default_transcription_language = WhisperSink.normalize_transcription_language(
            TRANSCRIPTION_LANGUAGE
        )
        if TRANSCRIPTION_METHOD == "openai":
            self.transcriber_type = "openai"
        else:
            self.transcriber_type = "local"
        if PLAYER_MAP_FILE_PATH:
            with open(PLAYER_MAP_FILE_PATH, "r", encoding="utf-8") as file:
                self.player_map = yaml.safe_load(file)

    async def on_ready(self):
        logger.info(f"Logged in as {self.user} to Discord.")
        self._is_ready = True

    async def close_consumers(self):
        await self.consumer_manager.close()

    def _close_and_clean_sink_for_guild(self, guild_id: int):
        whisper_sink: WhisperSink | None = self.guild_whisper_sinks.get(
            guild_id, None)

        if whisper_sink:
            logger.debug(f"Stopping whisper sink, requested by {guild_id}.")
            whisper_sink.stop_voice_thread()
            del self.guild_whisper_sinks[guild_id]
            whisper_sink.close()

    def start_recording(self, ctx: discord.context.ApplicationContext):
        """
        Start recording audio from the voice channel. Create a whisper sink
        and start sending transcripts to the queue.

        Since this is a critical function, this is where we should handle
        subscription checks and limits.
        """
        try:
            self.start_whisper_sink(ctx)
            self.guild_is_recording[ctx.guild_id] = True
        except Exception as e:
            logger.error(f"Error starting whisper sink: {e}")

    def start_whisper_sink(self, ctx: discord.context.ApplicationContext):
        guild_voice_sink = self.guild_whisper_sinks.get(ctx.guild_id, None)
        if guild_voice_sink:
            logger.debug(
                f"Sink is already active for guild {ctx.guild_id}.")
            return

        async def on_stop_record_callback(sink: WhisperSink, ctx):
            logger.debug(
                f"{ctx.channel.guild.id} -> on_stop_record_callback")
            self._close_and_clean_sink_for_guild(ctx.guild_id)

        transcript_queue = asyncio.Queue()

        whisper_sink = WhisperSink(
            transcript_queue,
            self.loop,
            data_length=50000,
            max_speakers=10,
            transcriber_type=self.transcriber_type,
            transcription_language=self.get_transcription_language(ctx.guild_id),
            player_map=self.player_map,
        )

        self.guild_to_helper[ctx.guild_id].vc.start_recording(
            whisper_sink, on_stop_record_callback, ctx)

        def on_thread_exception(e):
            logger.warning(
                f"Whisper sink thread exception for guild {ctx.guild_id}. Retry in 5 seconds...\n{e}")
            self._close_and_clean_sink_for_guild(ctx.guild_id)

            # retry in 5 seconds
            self.loop.call_later(5, self.start_recording, ctx)

        whisper_sink.start_voice_thread(on_exception=on_thread_exception)

        self.guild_whisper_sinks[ctx.guild_id] = whisper_sink

    def stop_recording(self, ctx: discord.context.ApplicationContext):
        vc = ctx.guild.voice_client
        if vc:
            self.guild_is_recording[ctx.guild_id] = False
            vc.stop_recording()
        guild_id = ctx.guild_id
        whisper_message_task = self.guild_whisper_message_tasks.get(
            guild_id, None)
        if whisper_message_task:
            logger.debug("Cancelling whisper message task.")
            whisper_message_task.cancel()
            del self.guild_whisper_message_tasks[guild_id]

    def cleanup_sink(self, ctx: discord.context.ApplicationContext):
        guild_id = ctx.guild_id
        self._close_and_clean_sink_for_guild(guild_id)

    def get_transcription_language(self, guild_id: int) -> str:
        return self.guild_transcription_languages.get(
            guild_id, self.default_transcription_language
        )

    def set_transcription_language(self, guild_id: int, language: str) -> str:
        normalized_language = WhisperSink.normalize_transcription_language(language)
        self.guild_transcription_languages[guild_id] = normalized_language

        running_sink = self.guild_whisper_sinks.get(guild_id)
        if running_sink:
            running_sink.set_transcription_language(normalized_language)

        return normalized_language

    @staticmethod
    def _format_timestamp(total_seconds: int) -> str:
        minutes, seconds = divmod(total_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    async def _resolve_display_name(self, guild: discord.Guild, user_id: int) -> str:
        member = guild.get_member(user_id)
        if member:
            return member.display_name
        try:
            member = await guild.fetch_member(user_id)
            return member.display_name
        except Exception:
            return str(user_id)

    async def _handle_transcription_item(self, ctx: discord.context.ApplicationContext, item: dict):
        session = self.active_sessions.get(ctx.guild_id)
        if not session:
            return

        payload = item.get("log", {}) if isinstance(item, dict) else {}
        text = (payload.get("data") or "").strip()
        if not text:
            return

        user_id = int(payload.get("user_id"))
        session.chunk_index += 1
        chunk_number = session.chunk_index

        wav_b64 = item.get("wav_b64", "")
        if wav_b64:
            user_dir = session.chunks_dir / str(user_id)
            user_dir.mkdir(parents=True, exist_ok=True)
            chunk_path = user_dir / f"chunk_{chunk_number:04d}.wav"
            chunk_path.write_bytes(base64.b64decode(wav_b64))

        display_name = await self._resolve_display_name(ctx.guild, user_id)
        session.display_names[user_id] = display_name

        t_seconds = (chunk_number - 1) * CHUNK_SECONDS
        timestamp = self._format_timestamp(t_seconds)
        session.transcript_lines.append(f"[{timestamp}] {display_name}: {text}")

    async def get_transcription(self, ctx: discord.context.ApplicationContext):
        # Get the transcription queue
        if not (self.guild_whisper_sinks.get(ctx.guild_id)):
            return
        whisper_sink = self.guild_whisper_sinks[ctx.guild_id]
        transcriptions = []
        if whisper_sink is None:
            return

        transcriptions_queue = whisper_sink.transcription_output_queue
        while not transcriptions_queue.empty():
            item = await transcriptions_queue.get()
            if isinstance(item, dict):
                await self._handle_transcription_item(ctx, item)
                log_payload = item.get("log", {})
                transcriptions.append(log_payload.get("data", ""))
            else:
                transcriptions.append(item)
        return transcriptions

    def start_session(self, guild_id: int) -> SessionContext:
        session = init_session(base_dir="/data/sessions")
        self.active_sessions[guild_id] = session
        logger.info("Session started for guild %s: %s", guild_id, session.session_id)
        return session

    def stop_session(self, guild_id: int) -> SessionContext | None:
        return self.active_sessions.pop(guild_id, None)

    def finalize_session(self, session: SessionContext) -> None:
        transcript_path = session.session_dir / "transcript.md"
        transcript_content = [f"# Transcript – {session.session_id}", ""]
        transcript_content.extend(session.transcript_lines)
        transcript_path.write_text("\n".join(transcript_content) + "\n", encoding="utf-8")

        user_outputs: list[Path] = []
        for user_dir in sorted(p for p in session.chunks_dir.iterdir() if p.is_dir()):
            wavs = sorted(user_dir.glob("chunk_*.wav"))
            if not wavs:
                continue
            user_id = int(user_dir.name)
            display_name = session.display_names.get(user_id, str(user_id))
            safe_name = safe_filename(display_name)
            out_ogg = session.audio_dir / f"user_{user_id}_{safe_name}_full.ogg"
            concat_wavs_to_opus_ogg(wavs, out_ogg, bitrate="32k")
            user_outputs.append(out_ogg)

        if not user_outputs:
            logger.warning("No audio chunk files available for session %s", session.session_id)
            return

        mixed_out = session.audio_dir / "mixed_full.ogg"
        mix_opus_ogg(user_outputs, mixed_out, bitrate="48k")

    def get_session_artifact_paths(self, session: SessionContext) -> list[Path]:
        artifacts = [
            session.session_dir / "transcript.md",
            session.audio_dir / "mixed_full.ogg",
        ]
        artifacts.extend(sorted(session.audio_dir.glob("user_*_full.ogg")))
        return [path for path in artifacts if path.exists()]

    async def update_player_map(self, ctx: discord.context.ApplicationContext):
        player_map = {}
        for member in ctx.guild.members:
            player_map[member.id] = {
                "player": member.name,
                "character": member.display_name
            }
        logger.info(f"{str(player_map)}")
        self.player_map.update(player_map)
        if PLAYER_MAP_FILE_PATH:
            with open(PLAYER_MAP_FILE_PATH, "w", encoding="utf-8") as file:
                yaml.dump(self.player_map, file, default_flow_style=False, allow_unicode=True)

    async def stop_and_cleanup(self):
        try:
            for sink in self.guild_whisper_sinks.values():
                sink.close()
                sink.stop_voice_thread()
                logger.debug(
                    f"Stopped whisper sink for guild {sink.vc.channel.guild.id} in cleanup.")
            self.guild_whisper_sinks.clear()
        except Exception as e:
            logger.error(f"Error stopping whisper sinks: {e}")
        finally:
            logger.info("Cleanup completed.")
