import asyncio
import logging
import os
import zipfile
from datetime import datetime
from pathlib import Path

import discord
from dotenv import load_dotenv

from src.bot.helper import BotHelper
from src.config.cliargs import CLIArgs
from src.utils.commandline import CommandLine
from src.utils.pdf_generator import pdf_generator

load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
PLAYER_MAP_FILE_PATH = os.getenv("PLAYER_MAP_FILE_PATH")

logger = logging.getLogger()  # root logger


def configure_logging():
    logging.getLogger('discord').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('faster_whisper').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)

    # Ensure the directory exists
    log_directory = '.logs/transcripts'
    pdf_directory = '.logs/pdfs'
    os.makedirs(log_directory, exist_ok=True) 
    os.makedirs(pdf_directory, exist_ok=True)  

    # Get the current date for the log file name
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = os.path.join(log_directory, f"{current_date}-transcription.log")

    # Custom logging format (date with milliseconds, message)
    log_format = '%(asctime)s %(name)s: %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S.%f'[:-3]  # Trim to milliseconds

    if CLIArgs.verbose:
        logger.setLevel(logging.DEBUG)
        logging.basicConfig(level=logging.DEBUG,
                            format=log_format,
                            datefmt=date_format)
    else:
        logger.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO,
                            format=log_format,
                            datefmt=date_format)
    
    # Set up the transcription logger
    transcription_logger = logging.getLogger('transcription')
    transcription_logger.setLevel(logging.INFO)

    # File handler for transcription logs (append mode)
    file_handler = logging.FileHandler(log_filename, mode='a')
    file_handler.setLevel(logging.INFO)
    
    # Custom formatter WITHOUT the automatic timestamp
    file_handler.setFormatter(logging.Formatter(
        '%(message)s'  # Only log the custom message, no automatic timestamp
    ))

    # Add the handler to the transcription logger
    transcription_logger.addHandler(file_handler)


def _get_stop_error_message(bot, ctx: discord.context.ApplicationContext) -> str | None:
    helper = bot.guild_to_helper.get(ctx.guild_id, None)
    if not helper:
        return "Well, that's akward. I dont seem to be in your party."

    if not helper.vc:
        return "Well, that's akward. I dont seem to be in your party."

    if not bot.guild_is_recording.get(ctx.guild_id, False):
        return "Well, that’s awkward. 😐 Was I suppose to be writing?"

    return None


async def _stop_recording_for_guild(bot, ctx: discord.context.ApplicationContext) -> None:
    await bot.get_transcription(ctx)
    bot.stop_recording(ctx)
    await bot.get_transcription(ctx)
    bot.guild_is_recording[ctx.guild_id] = False


def _finalize_session_and_collect_artifacts(bot, session) -> tuple[str, list[Path]]:
    try:
        bot.finalize_session(session)
    except Exception as e:
        logger.error(f"Audio export failed for session {session.session_id}: {e}")
        stop_message = (
            f"The quill rests. 🖋️ Transcript wurde gespeichert, aber Audioexport "
            f"fehlgeschlagen für `{session.session_id}`."
        )
    else:
        stop_message = (
            f"The quill rests. 🖋️ Session `{session.session_id}` gespeichert unter "
            f"`/data/sessions/{session.session_id}`."
        )

    return stop_message, bot.get_session_artifact_paths(session)


async def _post_session_artifacts(channel, session_id: str, artifact_paths: list[Path]) -> None:
    if not channel or not artifact_paths:
        return

    failed_artifacts = await _upload_artifact_files(channel, session_id, artifact_paths)
    if not failed_artifacts:
        return

    zip_uploaded = await _upload_zip_fallback(channel, session_id, failed_artifacts)
    await _post_upload_failure_notice(channel, session_id, failed_artifacts, zip_uploaded)


async def _upload_artifact_files(channel, session_id: str, artifact_paths: list[Path]) -> list[Path]:
    failed_artifacts: list[Path] = []
    for artifact_path in artifact_paths:
        if await _try_upload_artifact(channel, session_id, artifact_path):
            continue
        failed_artifacts.append(artifact_path)
    return failed_artifacts


async def _try_upload_artifact(channel, session_id: str, artifact_path: Path) -> bool:
    try:
        await channel.send(
            content=f"Session `{session_id}`: `{artifact_path.name}`",
            file=discord.File(str(artifact_path), filename=artifact_path.name),
        )
        return True
    except Exception as e:
        logger.error(f"Failed to upload session artifact {artifact_path}: {e}")
        return False


def _build_session_zip_path(session_id: str) -> Path:
    export_dir = Path(".logs/session_exports")
    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir / f"{session_id}_artifacts.zip"


def _create_artifact_zip(session_id: str, artifact_paths: list[Path]) -> Path | None:
    zip_path = _build_session_zip_path(session_id)
    try:
        if zip_path.exists():
            zip_path.unlink()
        with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zip_file:
            for artifact_path in artifact_paths:
                zip_file.write(artifact_path, arcname=artifact_path.name)
    except Exception as e:
        logger.error(f"Failed to create artifact ZIP for session {session_id}: {e}")
        if zip_path.exists():
            zip_path.unlink(missing_ok=True)
        return None
    return zip_path


async def _upload_zip_fallback(channel, session_id: str, artifact_paths: list[Path]) -> bool:
    zip_path = _create_artifact_zip(session_id, artifact_paths)
    if not zip_path:
        return False

    try:
        await channel.send(
            content=f"Session `{session_id}`: ZIP-Fallback mit fehlgeschlagenen Dateien",
            file=discord.File(str(zip_path), filename=zip_path.name),
        )
        return True
    except Exception as e:
        logger.error(f"Failed to upload ZIP fallback for session {session_id}: {e}")
        return False
    finally:
        zip_path.unlink(missing_ok=True)


async def _post_upload_failure_notice(
    channel,
    session_id: str,
    failed_artifacts: list[Path],
    zip_uploaded: bool,
) -> None:
    failed_names = ", ".join(f"`{path.name}`" for path in failed_artifacts)
    if zip_uploaded:
        await channel.send(
            f"Einige Dateien konnten einzeln nicht hochgeladen werden: {failed_names}. "
            f"Ich habe stattdessen ein ZIP-Fallback für Session `{session_id}` gepostet."
        )
        return

    await channel.send(
        f"Einige Session-Dateien konnten nicht in Discord hochgeladen werden: {failed_names}. "
        f"Lokal bleiben sie unter `/data/sessions/{session_id}` verfügbar."
    )


if __name__ == "__main__":
    args = CommandLine.read_command_line()
    CLIArgs.update_from_args(args)

    configure_logging()
    loop = asyncio.get_event_loop()
    
    from src.bot.volo_bot import VoloBot  
    
    bot = VoloBot(loop)

    @bot.event
    async def on_voice_state_update(member, before, after):
        if member.id == bot.user.id:
            # If the bot left the "before" channel
            if after.channel is None:
                guild_id = before.channel.guild.id
                helper = bot.guild_to_helper.get(guild_id, None)
                if helper:
                    helper.set_vc(None)
                    bot.guild_to_helper.pop(guild_id, None)

                bot._close_and_clean_sink_for_guild(guild_id)

    @bot.slash_command(name="connect", description="Add VOLO to your voice party.")
    async def connect(ctx: discord.context.ApplicationContext):
        if bot._is_ready is False:
            await ctx.respond("Ahem, seems even the finest quills falter. 🛑 No connection, no tale. Try again, my dear adventurer shortly.”", ephemeral=True)
            return
        author_vc = ctx.author.voice
        if not author_vc:
            await ctx.respond("I'm sorry adventurer, but it appears your voice has not joined a party.", ephemeral=True)
            return
        # check if we are already connected to a voice channel
        if bot.guild_to_helper.get(ctx.guild_id, None):
            await ctx.respond("I'm sorry adventurer, but it appears I'm already in a party. 🤺", ephemeral=True)
            return
        await ctx.trigger_typing()
        try:
            guild_id = ctx.guild_id
            vc = await author_vc.channel.connect()
            helper = bot.guild_to_helper.get(guild_id, BotHelper(bot))
            helper.guild_id = guild_id
            helper.set_vc(vc)
            bot.guild_to_helper[guild_id] = helper
            await ctx.respond(f"Ah, splendid! The lore shall now flow as freely as the finest ale. 🍺 Prepare to immortalize brilliance!", ephemeral=False)
            await ctx.guild.change_voice_state(channel=author_vc.channel, self_mute=True)
        except Exception as e:
            await ctx.respond(f"{e}", ephemeral=True)

    @bot.slash_command(name="language", description="Set transcription language (auto/de/eng).")
    @discord.option(
        "language",
        description="Choose transcription language",
        choices=["auto", "de", "eng"],
        required=True,
    )
    async def language(ctx: discord.context.ApplicationContext, language: str):
        selected_language = bot.set_transcription_language(ctx.guild_id, language)
        display_language = "eng" if selected_language == "en" else selected_language
        await ctx.respond(
            f"Transcription language set to `{display_language}`.",
            ephemeral=False,
        )

    @bot.slash_command(name="scribe", description="Ink the Saga of this adventure.")
    async def ink(ctx: discord.context.ApplicationContext):
        await ctx.trigger_typing()
        connect_command = next((cmd for cmd in ctx.bot.application_commands if cmd.name == "connect"), None)
        if not connect_command:
            connect_text = "`/connect`"
        else:
            connect_text = f"</connect:{connect_command.id}>"
        if not bot.guild_to_helper.get(ctx.guild_id, None):
            await ctx.respond(f"Well, that's akward. I dont seem to be in your party. How about I join? {connect_text}", ephemeral=True)
            return
        # check if we are already scribing
        if bot.guild_is_recording.get(ctx.guild_id, False):
            await ctx.respond("I'm sorry my liege, I can only write so fast.. 😥 ✒️", ephemeral=True)
            return
        session = bot.start_session(ctx.guild_id)
        bot.start_recording(ctx)
        await ctx.respond(f"Your words are now inscribed in the annals of history! ✍️ Session gestartet: `{session.session_id}`", ephemeral=False)
    
    @bot.slash_command(name="stop", description="Close the Tome on this adventure.")
    async def stop(ctx: discord.context.ApplicationContext):
        stop_error = _get_stop_error_message(bot, ctx)
        if stop_error:
            await ctx.respond(stop_error, ephemeral=True)
            return

        await ctx.trigger_typing()

        try:
            await _stop_recording_for_guild(bot, ctx)
            session = bot.stop_session(ctx.guild_id)
            if not session:
                await ctx.respond("The quill rests. 🖋️", ephemeral=False)
                return

            stop_message, artifact_paths = _finalize_session_and_collect_artifacts(
                bot, session
            )
            await ctx.respond(stop_message, ephemeral=False)
            await _post_session_artifacts(ctx.channel, session.session_id, artifact_paths)
        finally:
            bot.cleanup_sink(ctx)
        
    @bot.slash_command(name="disconnect", description="VOLO leaves your party. Goodbye, friend.")
    async def disconnect(ctx: discord.context.ApplicationContext):
        guild_id = ctx.guild_id
        id_exists = bot.guild_to_helper.get(guild_id, None)
        if not id_exists:
            await ctx.respond("Well, that's akward. I dont seem to be in your party... Should I just go?", ephemeral=True)
            return
        
        helper = bot.guild_to_helper[guild_id]    
        bot_vc = helper.vc
        
        if not bot_vc:
            await ctx.respond("Huh, weird.. where am I? Maybe we should party back up.", ephemeral=True)
            return
        
        await ctx.trigger_typing()
        await bot_vc.disconnect()
        helper.guild_id = None
        helper.set_vc(None)
        bot.guild_to_helper.pop(guild_id, None)

        await ctx.respond("The tome is sealed! 📖 Another chapter well-told, another adventure preserved. You have my gratitude!", ephemeral=False)

    @bot.slash_command(name="generate_pdf", description="Generate a PDF of the transcriptions.")
    async def generate_pdf(ctx: discord.context.ApplicationContext):
        guild_id = ctx.guild_id
        helper = bot.guild_to_helper.get(guild_id, None)
        if not helper:
            await ctx.respond("Well, that's akward. I dont seem to be in your party.", ephemeral=True)
            return
        transcription = await bot.get_transcription(ctx)
        if not transcription:
            await ctx.respond("I'm sorry, but it appears I have no transcriptions to write into the tome.", ephemeral=True)
            return
        pdf_file_path = await pdf_generator(transcription)
        # Send the PDF as an attachment
        if os.path.exists(pdf_file_path):
            try:
                with open(pdf_file_path, "rb") as f:
                    discord_file = discord.File(f, filename=f"session_transcription.pdf")
                    await ctx.respond("Here is the transcription from this session:", file=discord_file)
            finally:
                os.remove(pdf_file_path)
        else:
            await ctx.respond("No transcription file could be generated.", ephemeral=True)


    @bot.slash_command(name="update_player_map", description="Updates the player_map. If `PLAYER_MAP_FILE_PATH` is defined writes info to that location.")
    async def update_player_map(ctx: discord.context.ApplicationContext):
        if bot.guild_is_recording.get(ctx.guild_id, False):
            await ctx.respond("I'm sorry, I am already scribing for a set of true names ..", ephemeral=True)
            return
        try:
            await bot.update_player_map(ctx)
            await ctx.respond("📜✨ Behold, the Tome of True Names is Updated ✨📜")
        except Exception as e:
            await ctx.respond(f"Unable to update player_map.yml.:\n{e}", ephemeral=True)
            raise e


    @bot.slash_command(name="help", description="Show the help message.")
    async def help(ctx: discord.context.ApplicationContext):
        embed_fields = [
            discord.EmbedField(
                name="/connect", value="Connect to your voice channel.", inline=True),
            discord.EmbedField(
                name="/disconnect", value="Disconnect from your voice channel.", inline=True),
            discord.EmbedField(
                name="/scribe", value="Transcribe the voice channel.", inline=True),
            discord.EmbedField(
                name="/stop", value="Stop the transcription.", inline=True),
            discord.EmbedField(
                name="/language", value="Set transcription language (auto/de/eng).", inline=True),
            discord.EmbedField(
                name="/generate_pdf", value="Generate a PDF of the transcriptions.", inline=True),
            discord.EmbedField(
                name="/help", value="Show the help message.", inline=True),
        ]

        embed = discord.Embed(title="Volo Help 📖",
                              description="""Summon the Lorekeeper’s Wisdom 🔉 ➡️ 📃""",
                              color=discord.Color.blue(),
                              fields=embed_fields)

        await ctx.respond(embed=embed, ephemeral=True)



    try:
        loop.run_until_complete(bot.start(DISCORD_BOT_TOKEN))
    except KeyboardInterrupt:
        logger.info("^C received, shutting down...")
        asyncio.run(bot.stop_and_cleanup())
    finally:
        # Close all connections
        loop.run_until_complete(bot.close_consumers())

        tasks = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for task in tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

        # Close the loop
        loop.run_until_complete(bot.close())
        loop.close()
